package la.dere.nifi.zerobus;

import com.databricks.zerobus.IPCCompressionType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Configuration tests for PutZerobusRecord. Like its JSON sibling's tests,
 * these stop short of talking to Databricks — the Arrow conversion itself
 * is covered by {@link ArrowRecordConverterTest}.
 */
public class PutZerobusRecordTest {

    private TestRunner runner;

    @BeforeEach
    public void setUp() {
        runner = TestRunners.newTestRunner(PutZerobusRecord.class);
    }

    @Test
    public void testRequiredPropertiesNotSet() {
        runner.assertNotValid();
    }

    @Test
    public void testRecordReaderIsRequired() {
        configureConnectionProperties();
        runner.assertNotValid();
    }

    @Test
    public void testValidConfiguration() throws InitializationException {
        configureConnectionProperties();
        configureRecordReader();
        runner.assertValid();
    }

    @Test
    public void testDefaults() throws InitializationException {
        configureConnectionProperties();
        configureRecordReader();
        assertEquals("10000", runner.getProcessContext()
                .getProperty(PutZerobusRecord.RECORDS_PER_BATCH).getValue());
        assertEquals("1000", runner.getProcessContext()
                .getProperty(PutZerobusRecord.MAX_INFLIGHT_BATCHES).getValue());
        assertEquals("NONE", runner.getProcessContext()
                .getProperty(PutZerobusRecord.IPC_COMPRESSION).getValue());
    }

    @Test
    public void testCompressionValues() throws InitializationException {
        configureConnectionProperties();
        configureRecordReader();
        for (IPCCompressionType type : IPCCompressionType.values()) {
            runner.setProperty(PutZerobusRecord.IPC_COMPRESSION, type.name());
            runner.assertValid();
        }
        // Gzip is a fine codec, just not one Arrow IPC speaks
        runner.setProperty(PutZerobusRecord.IPC_COMPRESSION, "GZIP");
        runner.assertNotValid();
    }

    @Test
    public void testInvalidRecordsPerBatch() throws InitializationException {
        configureConnectionProperties();
        configureRecordReader();
        runner.setProperty(PutZerobusRecord.RECORDS_PER_BATCH, "0");
        runner.assertNotValid();
    }

    @Test
    public void testRelationships() {
        assertEquals(3, runner.getProcessor().getRelationships().size(),
                "Should have exactly 3 relationships");
        assertTrue(runner.getProcessor().getRelationships().contains(PutZerobusRecord.REL_SUCCESS));
        assertTrue(runner.getProcessor().getRelationships().contains(PutZerobusRecord.REL_FAILURE));
        assertTrue(runner.getProcessor().getRelationships().contains(PutZerobusRecord.REL_RETRY));
    }

    // ── Helpers ─────────────────────────────────────────────────────────────────

    private void configureRecordReader() throws InitializationException {
        final MockRecordParser reader = new MockRecordParser();
        runner.addControllerService("reader", reader);
        runner.enableControllerService(reader);
        runner.setProperty(PutZerobusRecord.RECORD_READER, "reader");
    }

    private void configureConnectionProperties() {
        runner.setProperty(PutZerobusIngest.SERVER_ENDPOINT,
                "https://1234567890.zerobus.us-west-2.cloud.databricks.com");
        runner.setProperty(PutZerobusIngest.WORKSPACE_URL,
                "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com");
        runner.setProperty(PutZerobusIngest.TABLE_NAME, "main.default.security_events");
        runner.setProperty(PutZerobusIngest.CLIENT_ID, "test-client-id");
        runner.setProperty(PutZerobusIngest.CLIENT_SECRET, "test-client-secret");
    }
}
