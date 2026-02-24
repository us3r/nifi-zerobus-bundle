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
import java.util.concurrent.CompletionException;

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

    public static final PropertyDescriptor SERVER_ENDPOINT = new PropertyDescriptor.Builder()
            .name("zerobus-server-endpoint")
            .displayName("Zerobus Server Endpoint")
            .description(
                "Zerobus server endpoint in the format: <workspace-id>.zerobus.<region>.cloud.databricks.com. "
                + "Do not include the https:// prefix."
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

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles successfully ingested and acknowledged by Zerobus")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that could not be ingested due to non-retriable errors (schema mismatch, auth failure)")
            .build();

    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("FlowFiles that failed due to transient errors and can be retried")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
            SERVER_ENDPOINT, WORKSPACE_URL, TABLE_NAME, CLIENT_ID, CLIENT_SECRET, BATCH_SIZE, MAX_INFLIGHT
    ));

    private static final Set<Relationship> RELATIONSHIPS;
    static {
        Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        rels.add(REL_RETRY);
        RELATIONSHIPS = Collections.unmodifiableSet(rels);
    }

    private volatile ZerobusSdk sdk;
    private volatile ZerobusJsonStream stream;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws Exception {
        final String endpoint = context.getProperty(SERVER_ENDPOINT).getValue();
        final String workspace = context.getProperty(WORKSPACE_URL).getValue();
        final String table = context.getProperty(TABLE_NAME).getValue();
        final String clientId = context.getProperty(CLIENT_ID).getValue();
        final String clientSecret = context.getProperty(CLIENT_SECRET).getValue();
        final int maxInflight = context.getProperty(MAX_INFLIGHT).asInteger();

        getLogger().info("Opening Zerobus stream to table {} via {}", new Object[]{table, endpoint});

        // Set the NAR classloader as context classloader so that SDK-spawned threads
        // (gRPC, callbacks) can find Zerobus classes via Thread.getContextClassLoader()
        final ClassLoader narClassLoader = this.getClass().getClassLoader();
        Thread.currentThread().setContextClassLoader(narClassLoader);

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
                        getLogger().debug("Zerobus ACK for offset {}", new Object[]{offsetId});
                    }

                    @Override
                    public void onError(long offsetId, String message) {
                        getLogger().warn("Zerobus error for offset {}: {}", new Object[]{offsetId, message});
                    }
                })
                .build();

        try {
            stream = sdk.createJsonStream(table, clientId, clientSecret, options).join();
            getLogger().info("Zerobus stream opened successfully to {}", new Object[]{table});
        } catch (CompletionException e) {
            closeQuietly();
            throw new ProcessException("Failed to open Zerobus stream: " + unwrap(e).getMessage(), unwrap(e));
        }
    }

    @OnStopped
    public void onStopped() {
        getLogger().info("Closing Zerobus stream");
        closeQuietly();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();

        List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles == null || flowFiles.isEmpty()) {
            return;
        }

        // Check stream health — recreate if needed
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

        // Read all FlowFile contents
        List<String> records = new ArrayList<>(flowFiles.size());
        for (FlowFile ff : flowFiles) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream((int) ff.getSize());
            session.exportTo(ff, baos);
            records.add(new String(baos.toByteArray(), StandardCharsets.UTF_8));
        }

        try {
            Optional<Long> lastOffset = stream.ingestRecordsOffset(records);
            if (lastOffset.isPresent()) {
                stream.waitForOffset(lastOffset.get());
            }
            session.transfer(flowFiles, REL_SUCCESS);
            getLogger().debug("Ingested {} records, last offset: {}",
                    new Object[]{records.size(), lastOffset.orElse(-1L)});

        } catch (NonRetriableException e) {
            getLogger().error("Non-retriable Zerobus error: {}", new Object[]{e.getMessage()});
            session.transfer(flowFiles, REL_FAILURE);

        } catch (Exception e) {
            getLogger().error("Transient Zerobus error: {}", new Object[]{e.getMessage()});
            session.transfer(flowFiles, REL_RETRY);
        }
    }

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

    private static Throwable unwrap(Throwable t) {
        while (t instanceof CompletionException && t.getCause() != null) {
            t = t.getCause();
        }
        return t;
    }
}
