package la.dere.nifi.zerobus;

import com.databricks.zerobus.AckCallback;
import com.databricks.zerobus.NonRetriableException;
import com.databricks.zerobus.StreamConfigurationOptions;
import com.databricks.zerobus.ZerobusJsonStream;
import com.databricks.zerobus.ZerobusSdk;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Streams JSON records into Databricks Delta tables via the Zerobus Ingest SDK.
 *
 * Think of it as a very determined courier that maintains a persistent gRPC
 * connection to your lakehouse and refuses to leave until every record is
 * safely acknowledged in its Delta table.
 *
 * <h3>How it works</h3>
 * <ol>
 *   <li>Opens a persistent gRPC stream on startup ({@code @OnScheduled})</li>
 *   <li>Pulls up to {@code batchSize} FlowFiles per trigger</li>
 *   <li>Validates JSON structure (catches the obviously-not-JSON before bothering the server)</li>
 *   <li>Ingests records and waits for server ACK with a configurable timeout</li>
 *   <li>Routes to success / failure / retry based on the outcome</li>
 * </ol>
 *
 * <h3>JNI classloader note</h3>
 * The Zerobus SDK uses Rust via JNI. Native threads spawned by Rust don't inherit
 * NiFi's NAR classloader, so we set TCCL before every SDK interaction. The Dockerfile
 * also places the SDK JAR on NiFi's system classpath as a belt-and-suspenders measure.
 * Yes, classloader issues in NiFi are basically a rite of passage.
 */
@Tags({"databricks", "zerobus", "delta", "lakehouse", "ingest", "streaming"})
@CapabilityDescription(
    "Streams JSON FlowFile content into a Databricks Delta table using the Zerobus Ingest SDK. "
    + "Opens a persistent gRPC stream on startup and reuses it across invocations for maximum throughput. "
    + "Each FlowFile is expected to contain a single JSON record matching the target table schema. "
    + "Supports batch ingestion with configurable batch sizes and offset-based delivery acknowledgment."
)
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@ReadsAttribute(attribute = "mime.type", description = "Expected to be application/json")
public class PutZerobusIngest extends AbstractProcessor {

    // ── Properties ──────────────────────────────────────────────────────────────

    public static final PropertyDescriptor SERVER_ENDPOINT = new PropertyDescriptor.Builder()
            .name("zerobus-server-endpoint")
            .displayName("Zerobus Server Endpoint")
            .description(
                "Zerobus server endpoint URL, e.g. https://<workspace-id>.zerobus.<region>.cloud.databricks.com"
            )
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor WORKSPACE_URL = new PropertyDescriptor.Builder()
            .name("databricks-workspace-url")
            .displayName("Workspace URL")
            .description("Databricks workspace URL, e.g. https://dbc-a1b2c3d4-e5f6.cloud.databricks.com")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("target-table")
            .displayName("Target Table")
            .description("Fully qualified Delta table name: catalog.schema.table")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor CLIENT_ID = new PropertyDescriptor.Builder()
            .name("client-id")
            .displayName("Service Principal Client ID")
            .description("OAuth 2.0 client ID of the Databricks service principal")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor CLIENT_SECRET = new PropertyDescriptor.Builder()
            .name("client-secret")
            .displayName("Service Principal Client Secret")
            .description("OAuth 2.0 client secret of the Databricks service principal")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("batch-size")
            .displayName("Batch Size")
            .description("Number of FlowFiles to batch into a single Zerobus ingest call")
            .required(false)
            .defaultValue("100")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_INFLIGHT = new PropertyDescriptor.Builder()
            .name("max-inflight-records")
            .displayName("Max Inflight Records")
            .description("Maximum number of records awaiting server acknowledgment before backpressure is applied")
            .required(false)
            .defaultValue("10000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor WAIT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("ack-wait-timeout")
            .displayName("ACK Wait Timeout")
            .description(
                "Maximum time to wait for server acknowledgment per batch. "
                + "If exceeded, the batch is routed to retry — the data may still arrive, "
                + "Zerobus just hasn't confirmed it yet."
            )
            .required(false)
            .defaultValue("30 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_FLOWFILE_SIZE = new PropertyDescriptor.Builder()
            .name("max-flowfile-size")
            .displayName("Max FlowFile Size")
            .description(
                "Maximum allowed FlowFile content size. Larger files are routed to failure "
                + "without being sent to Zerobus. Keeps heap usage predictable: "
                + "worst-case memory per batch = Batch Size × Max FlowFile Size."
            )
            .required(false)
            .defaultValue("1 MB")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    // ── Relationships ───────────────────────────────────────────────────────────

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles successfully ingested and acknowledged by Zerobus")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description(
                "FlowFiles that could not be ingested due to non-retriable errors "
                + "(schema mismatch, auth failure, oversized content, invalid JSON)"
            )
            .build();

    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("FlowFiles that failed due to transient errors and can be retried")
            .build();

    // ── Internals ───────────────────────────────────────────────────────────────

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
            SERVER_ENDPOINT, WORKSPACE_URL, TABLE_NAME, CLIENT_ID, CLIENT_SECRET,
            BATCH_SIZE, MAX_INFLIGHT, WAIT_TIMEOUT, MAX_FLOWFILE_SIZE
    ));

    private static final Set<Relationship> RELATIONSHIPS;
    static {
        Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        rels.add(REL_RETRY);
        RELATIONSHIPS = Collections.unmodifiableSet(rels);
    }

    // Guards stream lifecycle: create, recreate, close.
    // onTrigger grabs a local reference under this lock, then releases it
    // before the actual I/O — so we don't hold the lock during network calls.
    private final Object streamLock = new Object();

    private volatile ZerobusSdk sdk;
    private volatile ZerobusJsonStream stream;

    // Captures async errors from the SDK's AckCallback.
    // Just logging them and hoping someone reads the log is not a strategy,
    // so we surface them on the next onTrigger invocation.
    private final AtomicReference<String> lastAsyncError = new AtomicReference<>();

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
     * Opens the gRPC stream to Zerobus. Called once when the processor is scheduled.
     * If this fails, NiFi will keep retrying on its administrative yield interval —
     * which is a polite way of saying "try again in a minute, champ."
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) throws Exception {
        final String endpoint = context.getProperty(SERVER_ENDPOINT).getValue();
        final String workspace = context.getProperty(WORKSPACE_URL).getValue();
        final String table = context.getProperty(TABLE_NAME).getValue();
        final String clientId = context.getProperty(CLIENT_ID).getValue();
        final String clientSecret = context.getProperty(CLIENT_SECRET).getValue();
        final int maxInflight = context.getProperty(MAX_INFLIGHT).asInteger();

        getLogger().info("Opening Zerobus stream to table {} via {}", new Object[]{table, endpoint});

        // Set NAR classloader as TCCL so Rust JNI threads inherit it.
        // Without this, native callbacks get ClassNotFoundException —
        // which is the JVM's way of saying "who are you and how did you get in here?"
        final ClassLoader original = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        try {
            sdk = new ZerobusSdk(endpoint, workspace);

            StreamConfigurationOptions options = StreamConfigurationOptions.builder()
                    .setMaxInflightRecords(maxInflight)
                    .setRecovery(true)
                    .setRecoveryRetries(5)
                    .setRecoveryTimeoutMs(30000)
                    .setRecoveryBackoffMs(3000)
                    .setAckCallback(new AckCallback() {
                        @Override
                        public void onAck(long offsetId) {
                            // Stream recovered (if there was an error) — clear the flag
                            lastAsyncError.set(null);
                            getLogger().debug("Zerobus ACK for offset {}", new Object[]{offsetId});
                        }

                        @Override
                        public void onError(long offsetId, String message) {
                            // Stash the error for onTrigger to pick up.
                            // A smoke detector that only writes to a journal
                            // protects nobody — so we surface this proactively.
                            lastAsyncError.set("offset=" + offsetId + ": " + message);
                            getLogger().warn("Zerobus async error for offset {}: {}",
                                    new Object[]{offsetId, message});
                        }
                    })
                    .build();

            synchronized (streamLock) {
                stream = sdk.createJsonStream(table, clientId, clientSecret, options).join();
            }
            lastAsyncError.set(null);
            getLogger().info("Zerobus stream opened successfully to {}", new Object[]{table});
        } catch (CompletionException e) {
            closeQuietly();
            throw new ProcessException("Failed to open Zerobus stream: " + unwrap(e).getMessage(), unwrap(e));
        } finally {
            // Always restore TCCL — leaving someone else's classloader on a thread
            // is the Java equivalent of leaving the toilet seat up
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    /**
     * Tears down the gRPC stream and SDK. NiFi guarantees all onTrigger threads
     * have completed before calling this — so the synchronized block is strictly
     * for visibility, not contention.
     */
    @OnStopped
    public void onStopped() {
        getLogger().info("Closing Zerobus stream");
        synchronized (streamLock) {
            closeQuietly();
        }
    }

    // ── Trigger ─────────────────────────────────────────────────────────────────

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // TCCL dance — same reason as onScheduled, different act of the play
        final ClassLoader original = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        try {
            doTrigger(context, session);
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    private void doTrigger(final ProcessContext context, final ProcessSession session) {
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final long waitTimeoutMs = context.getProperty(WAIT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);
        final long maxSize = context.getProperty(MAX_FLOWFILE_SIZE).asDataSize(DataUnit.B).longValue();

        final List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles == null || flowFiles.isEmpty()) {
            return;
        }

        // Surface any async errors from the SDK callback.
        // These happened on a background thread, so onTrigger is the first
        // chance we get to tell someone who can actually do something about it.
        final String asyncError = lastAsyncError.getAndSet(null);
        if (asyncError != null) {
            getLogger().warn("Previous async error from Zerobus callback: {}", new Object[]{asyncError});
        }

        // Grab a local reference to the stream under the lock.
        // This prevents races with concurrent onTrigger threads and onStopped.
        // We release the lock before doing any I/O — holding a lock during
        // a network call is how you turn a race condition into a deadlock.
        final ZerobusJsonStream localStream;
        synchronized (streamLock) {
            if (stream == null || stream.isClosed()) {
                getLogger().warn("Zerobus stream is closed, attempting to recreate");
                try {
                    stream = sdk.recreateStream(stream).join();
                    getLogger().info("Zerobus stream recreated successfully");
                } catch (Exception e) {
                    getLogger().error("Failed to recreate Zerobus stream: {}", new Object[]{e.getMessage()});
                    session.transfer(flowFiles, REL_RETRY);
                    return;
                }
            }
            localStream = stream;
        }

        // Read and validate FlowFile contents.
        // We separate the wheat (valid JSON) from the chaff (oversized, not-JSON)
        // before talking to the server — no point wasting a round trip on garbage.
        final List<String> records = new ArrayList<>(flowFiles.size());
        final List<FlowFile> validFiles = new ArrayList<>(flowFiles.size());
        final List<FlowFile> rejected = new ArrayList<>();

        for (FlowFile ff : flowFiles) {
            // Size guard: prevents heap from blowing up when someone accidentally
            // routes a 2GB core dump through a JSON ingest processor
            if (ff.getSize() > maxSize) {
                getLogger().warn("FlowFile {} exceeds max size ({} > {} bytes), routing to failure",
                        new Object[]{ff.getId(), ff.getSize(), maxSize});
                rejected.add(ff);
                continue;
            }

            final ByteArrayOutputStream baos = new ByteArrayOutputStream((int) ff.getSize());
            session.exportTo(ff, baos);
            final String content = new String(baos.toByteArray(), StandardCharsets.UTF_8);

            // Quick structural check — not a full parse (we leave that honor to
            // the Zerobus server, which validates against the Delta schema anyway).
            // This just catches the obviously-not-JSON: empty strings, XML,
            // or your cat walking on the keyboard.
            if (!looksLikeJson(content)) {
                getLogger().warn("FlowFile {} does not look like JSON, routing to failure",
                        new Object[]{ff.getId()});
                rejected.add(ff);
                continue;
            }

            records.add(content);
            validFiles.add(ff);
        }

        // Route the rejects before we talk to the server
        if (!rejected.isEmpty()) {
            session.transfer(rejected, REL_FAILURE);
        }

        if (validFiles.isEmpty()) {
            return;
        }

        // Ship it. The moment of truth — or at least the moment of network I/O.
        try {
            final Optional<Long> lastOffset = localStream.ingestRecordsOffset(records);
            if (lastOffset.isPresent()) {
                waitWithTimeout(localStream, lastOffset.get(), waitTimeoutMs);
            }

            session.transfer(validFiles, REL_SUCCESS);
            getLogger().debug("Ingested {} records, last offset: {}",
                    new Object[]{records.size(), lastOffset.orElse(-1L)});

        } catch (NonRetriableException e) {
            // Schema mismatch, auth failure, etc. — no amount of retrying will fix this.
            // Route to failure so the operator can investigate (and maybe fix the schema).
            getLogger().error("Non-retriable Zerobus error: {}", new Object[]{e.getMessage()});
            session.transfer(validFiles, REL_FAILURE);

        } catch (Exception e) {
            // Transient error — network blip, server restart, cosmic ray.
            // Route to retry and hope for the best.
            getLogger().error("Transient Zerobus error: {}", new Object[]{unwrap(e).getMessage()});
            session.transfer(validFiles, REL_RETRY);
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────────

    /**
     * Waits for server ACK with a timeout, because waiting forever
     * is a virtue in meditation but a bug in production.
     *
     * Wraps the blocking {@code waitForOffset} in a CompletableFuture
     * so we can bail out if the server ghosts us.
     */
    private static void waitWithTimeout(ZerobusJsonStream s, long offset, long timeoutMs)
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
            // Don't cancel the future — the SDK may still complete the ACK internally.
            // We just stop waiting and route to retry.
            throw new ProcessException(
                "Timed out waiting for Zerobus ACK (offset " + offset
                + ", timeout " + timeoutMs + "ms). "
                + "The data may still arrive — Zerobus just hasn't confirmed it yet."
            );
        } catch (ExecutionException e) {
            // Unwrap the CompletionException wrapping from runAsync
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw new ProcessException("Unexpected error waiting for ACK", cause);
        }
    }

    /**
     * Quick structural check: does this string look like it could be JSON?
     *
     * Not a full parse — that would require adding a JSON library dependency
     * just to say "yep, that's JSON alright." We leave the real validation
     * to the Zerobus server, which checks against the Delta table schema.
     *
     * This catches: empty strings, XML, CSV, YAML, binary data, and any other
     * format that definitely isn't JSON. It won't catch syntactically broken JSON
     * like {@code {"name": "missing-closing-brace"} — but the server will.
     */
    static boolean looksLikeJson(String content) {
        if (content == null || content.isEmpty()) {
            return false;
        }
        final String trimmed = content.trim();
        if (trimmed.isEmpty()) {
            return false;
        }
        final char first = trimmed.charAt(0);
        final char last = trimmed.charAt(trimmed.length() - 1);
        return (first == '{' && last == '}') || (first == '[' && last == ']');
    }

    /**
     * Closes the stream and SDK without throwing.
     * Must be called under {@link #streamLock}.
     *
     * The double-null-check-and-catch pattern looks paranoid,
     * but when you're cleaning up JNI resources that talk to a Rust runtime
     * over gRPC, paranoia is just good engineering.
     */
    private void closeQuietly() {
        if (stream != null) {
            try {
                stream.close();
            } catch (Exception e) {
                getLogger().debug("Error closing Zerobus stream: {}", new Object[]{e.getMessage()});
            }
            stream = null;
        }
        if (sdk != null) {
            try {
                sdk.close();
            } catch (Exception e) {
                getLogger().debug("Error closing Zerobus SDK: {}", new Object[]{e.getMessage()});
            }
            sdk = null;
        }
    }

    /**
     * Unwraps nested CompletionException chains.
     * The Zerobus SDK loves wrapping exceptions like a Russian nesting doll —
     * this peels them until we find the real cause.
     */
    private static Throwable unwrap(Throwable t) {
        while (t instanceof CompletionException && t.getCause() != null) {
            t = t.getCause();
        }
        return t;
    }
}
