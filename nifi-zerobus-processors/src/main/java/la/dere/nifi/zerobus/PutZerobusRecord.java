package la.dere.nifi.zerobus;

import com.databricks.zerobus.IPCCompressionType;
import com.databricks.zerobus.NonRetriableException;
import com.databricks.zerobus.ZerobusArrowStream;
import com.databricks.zerobus.ZerobusSdk;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Streams records into Databricks Delta tables as Apache Arrow batches via
 * the Zerobus Arrow Flight path.
 *
 * Where {@link PutZerobusIngest} ships one JSON document per FlowFile, this one
 * reads any format a Record Reader understands (Avro, CSV, JSON, Parquet, ...),
 * packs the records into columnar batches and sends those instead.
 *
 * <h3>How it works</h3>
 * <ol>
 *   <li>Creates the SDK and an Arrow allocator on startup ({@code @OnScheduled})</li>
 *   <li>Opens the Arrow stream lazily, on the first FlowFile — the stream is bound
 *       to an Arrow schema, and we only learn that from the Record Reader</li>
 *   <li>Reads the FlowFile in chunks of {@code Records Per Batch} and ingests each chunk</li>
 *   <li>Waits for the server to acknowledge the last batch, then routes the FlowFile</li>
 * </ol>
 *
 * <h3>Delivery guarantee</h3>
 * At-least-once. If a FlowFile fails halfway, the batches already handed to the
 * SDK may still land in the table, and a retry will send them again.
 */
@TriggerSerially // ZerobusArrowStream is not thread-safe
@Tags({"databricks", "zerobus", "delta", "lakehouse", "ingest", "streaming", "record", "arrow"})
@CapabilityDescription(
    "Streams the records of a FlowFile into a Databricks Delta table using the Zerobus Ingest SDK over Arrow Flight. "
    + "Records are read with the configured Record Reader, converted to Apache Arrow batches and ingested in "
    + "columnar form. The record schema must match the target table: field names and types are mapped as-is "
    + "(STRING to LargeUtf8, TIMESTAMP to microseconds in UTC, DECIMAL as text). "
    + "Delivery is at-least-once: a FlowFile that fails partway may have some of its batches already ingested."
)
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttribute(attribute = "record.count", description = "Number of records ingested from the FlowFile")
@SeeAlso(PutZerobusIngest.class)
public class PutZerobusRecord extends AbstractProcessor {

    // ── Properties ──────────────────────────────────────────────────────────────

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Controller Service used to parse incoming FlowFiles and determine the record schema")
            .required(true)
            .identifiesControllerService(RecordReaderFactory.class)
            .build();

    public static final PropertyDescriptor RECORDS_PER_BATCH = new PropertyDescriptor.Builder()
            .name("records-per-batch")
            .displayName("Records Per Batch")
            .description(
                "Maximum number of records packed into a single Arrow batch. "
                + "Larger batches mean better throughput and more heap per FlowFile."
            )
            .required(false)
            .defaultValue("10000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_INFLIGHT_BATCHES = new PropertyDescriptor.Builder()
            .name("max-inflight-batches")
            .displayName("Max Inflight Batches")
            .description("Maximum number of batches awaiting server acknowledgment before backpressure is applied")
            .required(false)
            .defaultValue("1000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor IPC_COMPRESSION = new PropertyDescriptor.Builder()
            .name("ipc-compression")
            .displayName("IPC Compression")
            .description("Compression applied to Arrow batches on the wire. Trades CPU for bandwidth.")
            .required(false)
            .allowableValues(IPCCompressionType.class)
            .defaultValue(IPCCompressionType.NONE.name())
            .build();

    // ── Relationships ───────────────────────────────────────────────────────────

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles whose records were all ingested and acknowledged by Zerobus")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description(
                "FlowFiles that could not be ingested due to non-retriable errors "
                + "(unparseable content, unsupported or mismatched schema, auth failure)"
            )
            .build();

    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("FlowFiles that failed due to transient errors and can be retried")
            .build();

    // ── Internals ───────────────────────────────────────────────────────────────

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            PutZerobusIngest.SERVER_ENDPOINT, PutZerobusIngest.WORKSPACE_URL, PutZerobusIngest.TABLE_NAME,
            PutZerobusIngest.CLIENT_ID, PutZerobusIngest.CLIENT_SECRET,
            RECORD_READER, RECORDS_PER_BATCH, MAX_INFLIGHT_BATCHES, IPC_COMPRESSION,
            PutZerobusIngest.WAIT_TIMEOUT
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE, REL_RETRY);

    // Guards stream lifecycle against onStopped. @TriggerSerially already keeps
    // onTrigger single-threaded, so this is about visibility, not contention.
    private final Object streamLock = new Object();

    private volatile ZerobusSdk sdk;
    private volatile BufferAllocator allocator;
    private volatile ZerobusArrowStream stream;
    // The Arrow schema the current stream was opened with
    private volatile Schema streamSchema;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    // ── Lifecycle ───────────────────────────────────────────────────────────────

    /**
     * Creates the SDK and the Arrow allocator. The stream itself has to wait
     * for the first FlowFile, because that's where the schema comes from.
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final String endpoint = context.getProperty(PutZerobusIngest.SERVER_ENDPOINT).getValue();
        final String workspace = context.getProperty(PutZerobusIngest.WORKSPACE_URL).getValue();

        final ClassLoader original = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        try {
            allocator = newCheckedAllocator();
            sdk = new ZerobusSdk(endpoint, workspace, PutZerobusIngest.APPLICATION_NAME);
        } catch (RuntimeException | Error e) {
            synchronized (streamLock) {
                closeQuietly();
            }
            throw e;
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @OnStopped
    public void onStopped() {
        getLogger().info("Closing Zerobus Arrow stream");
        synchronized (streamLock) {
            closeQuietly();
        }
    }

    /**
     * Arrow needs reflective access to java.nio internals. Without the JVM flag it
     * blows up on the first real allocation with a rather cryptic error, so we
     * allocate a few bytes right away and translate the failure into plain English.
     */
    private static BufferAllocator newCheckedAllocator() {
        BufferAllocator created = null;
        try {
            created = new RootAllocator();
            created.buffer(8).close();
            return created;
        } catch (Throwable t) {
            if (created != null) {
                try {
                    created.close();
                } catch (Exception ignored) {
                    // nothing left to clean up
                }
            }
            throw new ProcessException("Apache Arrow could not allocate off-heap memory. "
                    + "NiFi must be started with --add-opens=java.base/java.nio=ALL-UNNAMED "
                    + "(add it as a java.arg line in conf/bootstrap.conf): " + t, t);
        }
    }

    // ── Trigger ─────────────────────────────────────────────────────────────────

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        // TCCL dance — native threads spawned by the SDK should see the NAR classloader
        final ClassLoader original = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        try {
            final long recordCount = ingest(context, session, flowFile);
            flowFile = session.putAttribute(flowFile, "record.count", String.valueOf(recordCount));
            session.getProvenanceReporter().send(flowFile, transitUri(context));
            session.transfer(flowFile, REL_SUCCESS);
            getLogger().debug("Ingested {} records from {}", recordCount, flowFile);

        } catch (NonRetriableException | MalformedRecordException | SchemaNotFoundException
                 | IllegalArgumentException e) {
            // Bad data, bad schema or bad credentials — retrying won't make any of them better
            getLogger().error("Non-retriable error ingesting {}: {}", flowFile, e.getMessage(), e);
            session.transfer(flowFile, REL_FAILURE);

        } catch (Exception e) {
            // Transient error — network blip, server restart, cosmic ray
            final Throwable cause = PutZerobusIngest.unwrap(e);
            getLogger().error("Transient error ingesting {}: {}", flowFile, cause.getMessage(), cause);
            session.transfer(session.penalize(flowFile), REL_RETRY);
            context.yield();

        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    private long ingest(final ProcessContext context, final ProcessSession session, final FlowFile flowFile)
            throws Exception {
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER)
                .asControllerService(RecordReaderFactory.class);
        final int recordsPerBatch = context.getProperty(RECORDS_PER_BATCH).asInteger();
        final long waitTimeoutMs = context.getProperty(PutZerobusIngest.WAIT_TIMEOUT)
                .asTimePeriod(TimeUnit.MILLISECONDS);

        final ZerobusArrowStream localStream;
        Optional<Long> lastOffset = Optional.empty();
        long recordCount = 0;

        try (InputStream in = session.read(flowFile);
             RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())) {

            final RecordSchema recordSchema = reader.getSchema();
            final Schema arrowSchema = ArrowRecordConverter.toArrowSchema(recordSchema);
            localStream = ensureStream(context, arrowSchema);

            try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
                int row = 0;
                Record record;
                while ((record = reader.nextRecord()) != null) {
                    if (row == 0) {
                        root.allocateNew();
                    }
                    ArrowRecordConverter.writeRecord(root, row++, record, recordSchema);
                    if (row == recordsPerBatch) {
                        root.setRowCount(row);
                        lastOffset = localStream.ingestBatch(root);
                        recordCount += row;
                        row = 0;
                    }
                }
                if (row > 0) {
                    root.setRowCount(row);
                    lastOffset = localStream.ingestBatch(root);
                    recordCount += row;
                }
            }
        }

        // ACKs are ordered, so confirming the last batch confirms them all
        if (lastOffset.isPresent()) {
            waitWithTimeout(localStream, lastOffset.get(), waitTimeoutMs);
        }
        return recordCount;
    }

    /**
     * Returns a stream that matches the given schema, opening or reopening it if needed.
     * A schema change closes the current stream (flushing whatever is pending) and opens
     * a new one — Arrow streams are married to their schema, no divorce without paperwork.
     */
    private ZerobusArrowStream ensureStream(final ProcessContext context, final Schema arrowSchema)
            throws Exception {
        synchronized (streamLock) {
            if (sdk == null) {
                throw new ProcessException("Processor is stopped");
            }
            try {
                if (stream != null && !arrowSchema.equals(streamSchema)) {
                    getLogger().info("Record schema changed, reopening Zerobus Arrow stream");
                    closeStreamQuietly();
                }
                if (stream == null) {
                    final String table = context.getProperty(PutZerobusIngest.TABLE_NAME).getValue();
                    getLogger().info("Opening Zerobus Arrow stream to table {}", table);
                    stream = sdk.streamBuilder()
                            .table(table)
                            .oauth(context.getProperty(PutZerobusIngest.CLIENT_ID).getValue(),
                                    context.getProperty(PutZerobusIngest.CLIENT_SECRET).getValue())
                            .recovery(true)
                            .recoveryRetries(5)
                            .recoveryTimeoutMs(30000)
                            .recoveryBackoffMs(3000)
                            .arrow(arrowSchema)
                            .maxInflightBatches(context.getProperty(MAX_INFLIGHT_BATCHES).asInteger())
                            .ipcCompression(IPCCompressionType.valueOf(
                                    context.getProperty(IPC_COMPRESSION).getValue()))
                            .build()
                            .join();
                    streamSchema = arrowSchema;
                } else if (stream.isClosed()) {
                    getLogger().warn("Zerobus Arrow stream is closed, attempting to recreate");
                    stream = sdk.recreateArrowStream(stream).join();
                }
                return stream;
            } catch (CompletionException e) {
                final Throwable cause = PutZerobusIngest.unwrap(e);
                if (cause instanceof Exception) {
                    throw (Exception) cause;
                }
                throw e;
            }
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────────

    private static String transitUri(final ProcessContext context) {
        return context.getProperty(PutZerobusIngest.SERVER_ENDPOINT).getValue()
                + "/" + context.getProperty(PutZerobusIngest.TABLE_NAME).getValue();
    }

    /**
     * Same idea as in {@link PutZerobusIngest}: wait for the ACK, but not forever.
     */
    private static void waitWithTimeout(final ZerobusArrowStream s, final long offset, final long timeoutMs)
            throws Exception {
        final CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            try {
                s.waitForOffset(offset);
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        });
        try {
            future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new ProcessException(
                "Timed out waiting for Zerobus ACK (offset " + offset
                + ", timeout " + timeoutMs + "ms). "
                + "The data may still arrive — Zerobus just hasn't confirmed it yet."
            );
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw new ProcessException("Unexpected error waiting for ACK", cause);
        }
    }

    /** Must be called under {@link #streamLock}. */
    private void closeStreamQuietly() {
        if (stream != null) {
            try {
                stream.close();
            } catch (Exception e) {
                getLogger().debug("Error closing Zerobus Arrow stream: {}", e.getMessage());
            }
            stream = null;
            streamSchema = null;
        }
    }

    /** Must be called under {@link #streamLock}. */
    private void closeQuietly() {
        closeStreamQuietly();
        if (sdk != null) {
            try {
                sdk.close();
            } catch (Exception e) {
                getLogger().debug("Error closing Zerobus SDK: {}", e.getMessage());
            }
            sdk = null;
        }
        if (allocator != null) {
            try {
                allocator.close();
            } catch (Exception e) {
                getLogger().debug("Error closing Arrow allocator: {}", e.getMessage());
            }
            allocator = null;
        }
    }
}
