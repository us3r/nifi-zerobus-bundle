package la.dere.nifi.zerobus;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Translates NiFi records into Arrow batches using the type mapping Zerobus expects
 * for Delta tables (see the "Type mappings" table in the Zerobus SDK README).
 *
 * <p>The mapping has a few surprises worth knowing about: STRING is {@code LargeUtf8}
 * (not {@code Utf8}), BINARY is {@code LargeBinary}, and DECIMAL travels as text.
 * TIMESTAMP is always microseconds in UTC.
 */
final class ArrowRecordConverter {

    private static final ArrowType TIMESTAMP_MICROS_UTC = new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");

    private ArrowRecordConverter() {
    }

    // ── Schema ──────────────────────────────────────────────────────────────────

    static Schema toArrowSchema(final RecordSchema recordSchema) {
        final List<Field> fields = new ArrayList<>(recordSchema.getFieldCount());
        for (RecordField rf : recordSchema.getFields()) {
            fields.add(toArrowField(rf.getFieldName(), rf.getDataType(), rf.isNullable()));
        }
        return new Schema(fields);
    }

    private static Field toArrowField(final String name, final DataType dataType, final boolean nullable) {
        switch (dataType.getFieldType()) {
            case BOOLEAN:
                return leaf(name, ArrowType.Bool.INSTANCE, nullable);
            case BYTE:
                return leaf(name, new ArrowType.Int(8, true), nullable);
            case SHORT:
                return leaf(name, new ArrowType.Int(16, true), nullable);
            case INT:
                return leaf(name, new ArrowType.Int(32, true), nullable);
            case LONG:
            case BIGINT:
                return leaf(name, new ArrowType.Int(64, true), nullable);
            case FLOAT:
                return leaf(name, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), nullable);
            case DOUBLE:
                return leaf(name, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), nullable);
            case STRING:
            case CHAR:
            case ENUM:
            case UUID:
            case DECIMAL:
            case TIME:
                return leaf(name, ArrowType.LargeUtf8.INSTANCE, nullable);
            case DATE:
                return leaf(name, new ArrowType.Date(DateUnit.DAY), nullable);
            case TIMESTAMP:
                return leaf(name, TIMESTAMP_MICROS_UTC, nullable);
            case ARRAY: {
                final DataType elementType = ((ArrayDataType) dataType).getElementType();
                // NiFi models binary content as ARRAY<BYTE>
                if (isBinary(dataType)) {
                    return leaf(name, ArrowType.LargeBinary.INSTANCE, nullable);
                }
                requireType(name, elementType);
                return new Field(name, new FieldType(nullable, ArrowType.List.INSTANCE, null),
                        Collections.singletonList(toArrowField("item", elementType, true)));
            }
            case MAP: {
                final DataType valueType = ((MapDataType) dataType).getValueType();
                requireType(name, valueType);
                final Field entries = new Field("entries", FieldType.notNullable(ArrowType.Struct.INSTANCE),
                        Arrays.asList(
                                leaf("keys", ArrowType.LargeUtf8.INSTANCE, false),
                                toArrowField("values", valueType, true)));
                return new Field(name, new FieldType(nullable, new ArrowType.Map(false), null),
                        Collections.singletonList(entries));
            }
            case RECORD: {
                final RecordSchema childSchema = ((RecordDataType) dataType).getChildSchema();
                if (childSchema == null) {
                    throw new IllegalArgumentException("Field '" + name + "' is a RECORD without a schema");
                }
                final List<Field> children = new ArrayList<>(childSchema.getFieldCount());
                for (RecordField rf : childSchema.getFields()) {
                    children.add(toArrowField(rf.getFieldName(), rf.getDataType(), rf.isNullable()));
                }
                return new Field(name, new FieldType(nullable, ArrowType.Struct.INSTANCE, null), children);
            }
            default:
                // CHOICE lands here: Arrow needs exactly one type per column, and guessing
                // which branch of a union the Delta table wants is a great way to be wrong.
                throw new IllegalArgumentException("Field '" + name + "' has unsupported type "
                        + dataType.getFieldType() + "; configure the Record Reader with an explicit schema");
        }
    }

    private static Field leaf(final String name, final ArrowType type, final boolean nullable) {
        return new Field(name, new FieldType(nullable, type, null), null);
    }

    private static void requireType(final String name, final DataType type) {
        if (type == null) {
            throw new IllegalArgumentException("Field '" + name + "' has no element type");
        }
    }

    private static boolean isBinary(final DataType dataType) {
        if (dataType.getFieldType() != RecordFieldType.ARRAY) {
            return false;
        }
        final DataType elementType = ((ArrayDataType) dataType).getElementType();
        return elementType != null && elementType.getFieldType() == RecordFieldType.BYTE;
    }

    // ── Values ──────────────────────────────────────────────────────────────────

    /**
     * Writes one record into the given row of the batch. The caller owns the row count.
     */
    static void writeRecord(final VectorSchemaRoot root, final int row, final Record record,
                            final RecordSchema recordSchema) {
        final List<FieldVector> vectors = root.getFieldVectors();
        final List<RecordField> fields = recordSchema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            final RecordField rf = fields.get(i);
            writeValue(vectors.get(i), row, record.getValue(rf), rf.getDataType(), rf.getFieldName());
        }
    }

    private static void writeValue(final FieldVector vector, final int index, final Object value,
                                   final DataType dataType, final String name) {
        if (value == null) {
            if (!vector.getField().isNullable()) {
                throw new IllegalArgumentException("Field '" + name + "' is not nullable but the record has no value");
            }
            vector.setNull(index);
            return;
        }

        try {
            switch (dataType.getFieldType()) {
                case BOOLEAN:
                    ((BitVector) vector).setSafe(index, toBoolean(value) ? 1 : 0);
                    return;
                case BYTE:
                    ((TinyIntVector) vector).setSafe(index, toNumber(value).byteValue());
                    return;
                case SHORT:
                    ((SmallIntVector) vector).setSafe(index, toNumber(value).shortValue());
                    return;
                case INT:
                    ((IntVector) vector).setSafe(index, toNumber(value).intValue());
                    return;
                case LONG:
                case BIGINT:
                    ((BigIntVector) vector).setSafe(index, toNumber(value).longValue());
                    return;
                case FLOAT:
                    ((Float4Vector) vector).setSafe(index, toNumber(value).floatValue());
                    return;
                case DOUBLE:
                    ((Float8Vector) vector).setSafe(index, toNumber(value).doubleValue());
                    return;
                case DECIMAL:
                    setText(vector, index, value instanceof BigDecimal
                            ? ((BigDecimal) value).toPlainString() : value.toString());
                    return;
                case STRING:
                case CHAR:
                case ENUM:
                case UUID:
                case TIME:
                    setText(vector, index, value.toString());
                    return;
                case DATE:
                    ((DateDayVector) vector).setSafe(index, toEpochDay(value));
                    return;
                case TIMESTAMP:
                    ((TimeStampMicroTZVector) vector).setSafe(index, toEpochMicros(value));
                    return;
                case ARRAY:
                    if (isBinary(dataType)) {
                        ((LargeVarBinaryVector) vector).setSafe(index, toBytes(value));
                    } else {
                        writeList((ListVector) vector, index, toList(value),
                                ((ArrayDataType) dataType).getElementType(), name);
                    }
                    return;
                case MAP:
                    writeMap((MapVector) vector, index, (Map<?, ?>) value,
                            ((MapDataType) dataType).getValueType(), name);
                    return;
                case RECORD:
                    writeStruct((StructVector) vector, index, (Record) value,
                            ((RecordDataType) dataType).getChildSchema());
                    return;
                default:
                    throw new IllegalArgumentException("unsupported type " + dataType.getFieldType());
            }
        } catch (ClassCastException | ArithmeticException | java.time.DateTimeException e) {
            throw new IllegalArgumentException("Field '" + name + "': cannot convert value of type "
                    + value.getClass().getName() + " to " + dataType.getFieldType(), e);
        }
    }

    private static void writeList(final ListVector vector, final int index, final List<?> values,
                                  final DataType elementType, final String name) {
        final int start = vector.startNewValue(index);
        final FieldVector data = vector.getDataVector();
        for (int i = 0; i < values.size(); i++) {
            writeValue(data, start + i, values.get(i), elementType, name);
        }
        vector.endValue(index, values.size());
    }

    private static void writeMap(final MapVector vector, final int index, final Map<?, ?> map,
                                 final DataType valueType, final String name) {
        final int start = vector.startNewValue(index);
        final StructVector entries = (StructVector) vector.getDataVector();
        final List<FieldVector> children = entries.getChildrenFromFields();
        final FieldVector keys = children.get(0);
        final FieldVector values = children.get(1);
        int i = start;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            entries.setIndexDefined(i);
            setText(keys, i, String.valueOf(entry.getKey()));
            writeValue(values, i, entry.getValue(), valueType, name);
            i++;
        }
        vector.endValue(index, map.size());
    }

    private static void writeStruct(final StructVector vector, final int index, final Record record,
                                    final RecordSchema schema) {
        vector.setIndexDefined(index);
        final List<FieldVector> children = vector.getChildrenFromFields();
        final List<RecordField> fields = schema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            final RecordField rf = fields.get(i);
            writeValue(children.get(i), index, record.getValue(rf), rf.getDataType(), rf.getFieldName());
        }
    }

    private static void setText(final FieldVector vector, final int index, final String text) {
        ((LargeVarCharVector) vector).setSafe(index, text.getBytes(StandardCharsets.UTF_8));
    }

    // ── Coercion ────────────────────────────────────────────────────────────────
    // Record Readers usually hand over values already matching the schema, but
    // "usually" is doing some heavy lifting there, so we stay lenient.

    private static Number toNumber(final Object value) {
        if (value instanceof Number) {
            return (Number) value;
        }
        if (value instanceof CharSequence) {
            return new BigDecimal(value.toString().trim());
        }
        throw new ClassCastException("not a number");
    }

    private static boolean toBoolean(final Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof CharSequence) {
            return Boolean.parseBoolean(value.toString().trim());
        }
        throw new ClassCastException("not a boolean");
    }

    private static int toEpochDay(final Object value) {
        if (value instanceof LocalDate) {
            return Math.toIntExact(((LocalDate) value).toEpochDay());
        }
        if (value instanceof java.sql.Date) {
            return Math.toIntExact(((java.sql.Date) value).toLocalDate().toEpochDay());
        }
        if (value instanceof java.util.Date) {
            final Instant instant = Instant.ofEpochMilli(((java.util.Date) value).getTime());
            return Math.toIntExact(instant.atZone(ZoneId.systemDefault()).toLocalDate().toEpochDay());
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        if (value instanceof CharSequence) {
            return Math.toIntExact(LocalDate.parse(value.toString().trim()).toEpochDay());
        }
        throw new ClassCastException("not a date");
    }

    private static long toEpochMicros(final Object value) {
        if (value instanceof java.sql.Timestamp) {
            return micros(((java.sql.Timestamp) value).toInstant());
        }
        if (value instanceof java.util.Date) {
            return Math.multiplyExact(((java.util.Date) value).getTime(), 1000L);
        }
        if (value instanceof Instant) {
            return micros((Instant) value);
        }
        if (value instanceof LocalDateTime) {
            return micros(((LocalDateTime) value).atZone(ZoneId.systemDefault()).toInstant());
        }
        if (value instanceof java.time.OffsetDateTime) {
            return micros(((java.time.OffsetDateTime) value).toInstant());
        }
        if (value instanceof java.time.ZonedDateTime) {
            return micros(((java.time.ZonedDateTime) value).toInstant());
        }
        if (value instanceof Number) {
            // NiFi convention: numeric timestamps are epoch milliseconds
            return Math.multiplyExact(((Number) value).longValue(), 1000L);
        }
        if (value instanceof CharSequence) {
            return micros(Instant.parse(value.toString().trim()));
        }
        throw new ClassCastException("not a timestamp");
    }

    private static long micros(final Instant instant) {
        return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000L), instant.getNano() / 1000L);
    }

    private static byte[] toBytes(final Object value) {
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        if (value instanceof CharSequence) {
            return value.toString().getBytes(StandardCharsets.UTF_8);
        }
        final List<?> list = toList(value);
        final byte[] bytes = new byte[list.size()];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = ((Number) list.get(i)).byteValue();
        }
        return bytes;
    }

    private static List<?> toList(final Object value) {
        if (value instanceof Object[]) {
            return Arrays.asList((Object[]) value);
        }
        if (value instanceof List) {
            return (List<?>) value;
        }
        if (value instanceof Collection) {
            return new ArrayList<>((Collection<?>) value);
        }
        throw new ClassCastException("not an array");
    }
}
