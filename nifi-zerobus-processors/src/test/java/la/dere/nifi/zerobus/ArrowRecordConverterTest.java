package la.dere.nifi.zerobus;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks that NiFi records end up as the Arrow types Zerobus expects for Delta tables,
 * and that the values survive the trip.
 */
public class ArrowRecordConverterTest {

    private BufferAllocator allocator;

    @BeforeEach
    public void setUp() {
        allocator = new RootAllocator();
    }

    @AfterEach
    public void tearDown() {
        // Also catches leaks: closing an allocator with outstanding buffers throws
        allocator.close();
    }

    // ── Schema mapping ──────────────────────────────────────────────────────────

    @Test
    public void testScalarTypeMapping() {
        final Schema schema = ArrowRecordConverter.toArrowSchema(scalarSchema());

        assertEquals(ArrowType.Bool.INSTANCE, type(schema, "flag"));
        assertEquals(new ArrowType.Int(8, true), type(schema, "tiny"));
        assertEquals(new ArrowType.Int(16, true), type(schema, "small"));
        assertEquals(new ArrowType.Int(32, true), type(schema, "num"));
        assertEquals(new ArrowType.Int(64, true), type(schema, "big"));
        assertEquals(ArrowType.LargeUtf8.INSTANCE, type(schema, "name"));
        // DECIMAL travels as text on the Zerobus Arrow path
        assertEquals(ArrowType.LargeUtf8.INSTANCE, type(schema, "amount"));
        assertEquals(new ArrowType.Date(DateUnit.DAY), type(schema, "day"));
        assertEquals(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"), type(schema, "ts"));
        assertEquals(ArrowType.LargeBinary.INSTANCE, type(schema, "blob"));
    }

    @Test
    public void testNullabilityIsPreserved() {
        final RecordSchema recordSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("required", RecordFieldType.INT.getDataType(), false),
                new RecordField("optional", RecordFieldType.INT.getDataType(), true)));
        final Schema schema = ArrowRecordConverter.toArrowSchema(recordSchema);

        assertFalse(schema.findField("required").isNullable());
        assertTrue(schema.findField("optional").isNullable());
    }

    @Test
    public void testNestedTypeMapping() {
        final Schema schema = ArrowRecordConverter.toArrowSchema(nestedSchema());

        final Field tags = schema.findField("tags");
        assertEquals(ArrowType.List.INSTANCE, tags.getType());
        assertEquals("item", tags.getChildren().get(0).getName());
        assertEquals(ArrowType.LargeUtf8.INSTANCE, tags.getChildren().get(0).getType());

        final Field attrs = schema.findField("attrs");
        assertTrue(attrs.getType() instanceof ArrowType.Map);
        final Field entries = attrs.getChildren().get(0);
        assertEquals("entries", entries.getName());
        assertEquals("keys", entries.getChildren().get(0).getName());
        assertEquals("values", entries.getChildren().get(1).getName());
        assertEquals(new ArrowType.Int(64, true), entries.getChildren().get(1).getType());

        final Field geo = schema.findField("geo");
        assertEquals(ArrowType.Struct.INSTANCE, geo.getType());
        assertEquals("lat", geo.getChildren().get(0).getName());
    }

    @Test
    public void testChoiceTypeIsRejected() {
        // A column can't be "int or string, we'll see" — the reader needs an explicit schema
        final RecordSchema recordSchema = new SimpleRecordSchema(List.of(
                new RecordField("ambiguous", RecordFieldType.CHOICE.getChoiceDataType(
                        RecordFieldType.INT.getDataType(), RecordFieldType.STRING.getDataType()))));
        assertThrows(IllegalArgumentException.class, () -> ArrowRecordConverter.toArrowSchema(recordSchema));
    }

    // ── Values ──────────────────────────────────────────────────────────────────

    @Test
    public void testScalarValuesRoundTrip() {
        final RecordSchema recordSchema = scalarSchema();
        final Map<String, Object> values = new HashMap<>();
        values.put("flag", true);
        values.put("tiny", (byte) 7);
        values.put("small", (short) 300);
        values.put("num", 42);
        values.put("big", 1L << 40);
        values.put("ratio", 1.5f);
        values.put("score", 2.25d);
        values.put("name", "zażółć");
        values.put("amount", new BigDecimal("12345.6700"));
        values.put("day", java.sql.Date.valueOf(LocalDate.of(2026, 10, 1)));
        values.put("ts", Timestamp.from(Instant.parse("2026-10-01T12:00:00.123456Z")));
        values.put("blob", new Object[]{(byte) 1, (byte) 2, (byte) 3});

        try (VectorSchemaRoot root = write(recordSchema, new MapRecord(recordSchema, values))) {
            assertEquals(1, root.getRowCount());
            assertEquals(true, root.getVector("flag").getObject(0));
            assertEquals((byte) 7, root.getVector("tiny").getObject(0));
            assertEquals((short) 300, root.getVector("small").getObject(0));
            assertEquals(42, root.getVector("num").getObject(0));
            assertEquals(1L << 40, root.getVector("big").getObject(0));
            assertEquals(1.5f, root.getVector("ratio").getObject(0));
            assertEquals(2.25d, root.getVector("score").getObject(0));
            assertEquals("zażółć", root.getVector("name").getObject(0).toString());
            assertEquals("12345.6700", root.getVector("amount").getObject(0).toString());
            assertEquals((int) LocalDate.of(2026, 10, 1).toEpochDay(), root.getVector("day").getObject(0));
            assertEquals(1790856000123456L, root.getVector("ts").getObject(0));
            assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) root.getVector("blob").getObject(0));
        }
    }

    @Test
    public void testNullsAndLenientCoercion() {
        final RecordSchema recordSchema = scalarSchema();
        final Map<String, Object> values = new HashMap<>();
        // Readers don't always deliver the exact Java type the schema promises
        values.put("num", "42");
        values.put("big", 7);
        values.put("day", "2026-10-01");
        values.put("ts", 1000L);
        values.put("blob", "abc".getBytes(StandardCharsets.UTF_8));

        try (VectorSchemaRoot root = write(recordSchema, new MapRecord(recordSchema, values))) {
            assertNull(root.getVector("flag").getObject(0));
            assertNull(root.getVector("name").getObject(0));
            assertEquals(42, root.getVector("num").getObject(0));
            assertEquals(7L, root.getVector("big").getObject(0));
            assertEquals((int) LocalDate.of(2026, 10, 1).toEpochDay(), root.getVector("day").getObject(0));
            assertEquals(1_000_000L, root.getVector("ts").getObject(0));
            assertArrayEquals("abc".getBytes(StandardCharsets.UTF_8), (byte[]) root.getVector("blob").getObject(0));
        }
    }

    @Test
    public void testNestedValuesRoundTrip() {
        final RecordSchema recordSchema = nestedSchema();
        final RecordSchema geoSchema = geoSchema();

        final Map<String, Object> first = new HashMap<>();
        first.put("tags", new Object[]{"a", null, "c"});
        final Map<String, Object> attrs = new LinkedHashMap<>();
        attrs.put("x", 1L);
        attrs.put("y", 2L);
        first.put("attrs", attrs);
        first.put("geo", new MapRecord(geoSchema, Map.of("lat", 52.2, "lon", 21.0)));

        // Second row is all nulls — offsets of the first row must stay intact
        try (VectorSchemaRoot root = write(recordSchema,
                new MapRecord(recordSchema, first), new MapRecord(recordSchema, new HashMap<>()))) {
            assertEquals(2, root.getRowCount());

            final JsonStringArrayList<?> tags = (JsonStringArrayList<?>) root.getVector("tags").getObject(0);
            assertEquals(3, tags.size());
            assertEquals("a", tags.get(0).toString());
            assertNull(tags.get(1));
            assertEquals("c", tags.get(2).toString());

            final List<?> entries = (List<?>) root.getVector("attrs").getObject(0);
            assertEquals(2, entries.size());
            final Map<?, ?> entry = (Map<?, ?>) entries.get(1);
            assertEquals("y", entry.get("keys").toString());
            assertEquals(2L, entry.get("values"));

            final Map<?, ?> geo = (Map<?, ?>) root.getVector("geo").getObject(0);
            assertEquals(52.2, geo.get("lat"));
            assertEquals(21.0, geo.get("lon"));

            assertNull(root.getVector("tags").getObject(1));
            assertNull(root.getVector("attrs").getObject(1));
            assertNull(root.getVector("geo").getObject(1));
        }
    }

    @Test
    public void testNullInRequiredFieldIsRejected() {
        final RecordSchema recordSchema = new SimpleRecordSchema(List.of(
                new RecordField("required", RecordFieldType.INT.getDataType(), false)));
        final Record record = new MapRecord(recordSchema, new HashMap<>());
        assertThrows(IllegalArgumentException.class, () -> write(recordSchema, record).close());
    }

    @Test
    public void testUnconvertibleValueIsRejected() {
        final RecordSchema recordSchema = new SimpleRecordSchema(List.of(
                new RecordField("num", RecordFieldType.INT.getDataType())));
        final Record record = new MapRecord(recordSchema, Map.of("num", new Object[]{"nope"}));
        assertThrows(IllegalArgumentException.class, () -> write(recordSchema, record).close());
    }

    @Test
    public void testBatchCanBeRefilled() {
        // The processor reuses one VectorSchemaRoot across batches of a FlowFile
        final RecordSchema recordSchema = nestedSchema();
        final Schema schema = ArrowRecordConverter.toArrowSchema(recordSchema);
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            for (int batch = 0; batch < 3; batch++) {
                root.allocateNew();
                for (int row = 0; row < 100; row++) {
                    final Map<String, Object> values = new HashMap<>();
                    values.put("tags", new Object[]{"b" + batch, "r" + row});
                    ArrowRecordConverter.writeRecord(root, row, new MapRecord(recordSchema, values), recordSchema);
                }
                root.setRowCount(100);
                final List<?> tags = (List<?>) root.getVector("tags").getObject(99);
                assertEquals("b" + batch, tags.get(0).toString());
                assertEquals("r99", tags.get(1).toString());
            }
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────────────────

    private VectorSchemaRoot write(final RecordSchema recordSchema, final Record... records) {
        final VectorSchemaRoot root = VectorSchemaRoot.create(ArrowRecordConverter.toArrowSchema(recordSchema), allocator);
        try {
            root.allocateNew();
            for (int i = 0; i < records.length; i++) {
                ArrowRecordConverter.writeRecord(root, i, records[i], recordSchema);
            }
            root.setRowCount(records.length);
            return root;
        } catch (RuntimeException e) {
            root.close();
            throw e;
        }
    }

    private static ArrowType type(final Schema schema, final String name) {
        return schema.findField(name).getType();
    }

    private static RecordSchema scalarSchema() {
        return new SimpleRecordSchema(Arrays.asList(
                new RecordField("flag", RecordFieldType.BOOLEAN.getDataType()),
                new RecordField("tiny", RecordFieldType.BYTE.getDataType()),
                new RecordField("small", RecordFieldType.SHORT.getDataType()),
                new RecordField("num", RecordFieldType.INT.getDataType()),
                new RecordField("big", RecordFieldType.LONG.getDataType()),
                new RecordField("ratio", RecordFieldType.FLOAT.getDataType()),
                new RecordField("score", RecordFieldType.DOUBLE.getDataType()),
                new RecordField("name", RecordFieldType.STRING.getDataType()),
                new RecordField("amount", RecordFieldType.DECIMAL.getDecimalDataType(18, 4)),
                new RecordField("day", RecordFieldType.DATE.getDataType()),
                new RecordField("ts", RecordFieldType.TIMESTAMP.getDataType()),
                new RecordField("blob", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType()))));
    }

    private static RecordSchema geoSchema() {
        return new SimpleRecordSchema(Arrays.asList(
                new RecordField("lat", RecordFieldType.DOUBLE.getDataType()),
                new RecordField("lon", RecordFieldType.DOUBLE.getDataType())));
    }

    private static RecordSchema nestedSchema() {
        return new SimpleRecordSchema(Arrays.asList(
                new RecordField("tags", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType())),
                new RecordField("attrs", RecordFieldType.MAP.getMapDataType(RecordFieldType.LONG.getDataType())),
                new RecordField("geo", RecordFieldType.RECORD.getRecordDataType(geoSchema()))));
    }
}
