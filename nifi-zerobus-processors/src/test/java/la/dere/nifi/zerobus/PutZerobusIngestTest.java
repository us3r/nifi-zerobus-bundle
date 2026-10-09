package la.dere.nifi.zerobus;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for PutZerobusIngest processor.
 *
 * These tests validate processor configuration, property handling, and
 * the JSON structural validator. They don't require a live Databricks
 * workspace — that would be an integration test, and we have those too
 * (see: {@code mvn verify -Pit}).
 *
 * If you're reading this because a test failed: don't panic.
 * Read the assertion message, check what you changed, and remember
 * that tests are just code that judges your other code.
 */
public class PutZerobusIngestTest {

    private TestRunner runner;

    @BeforeEach
    public void setUp() {
        runner = TestRunners.newTestRunner(PutZerobusIngest.class);
    }

    // ── Processor loading ───────────────────────────────────────────────────────

    @Test
    public void testProcessorLoads() {
        assertNotNull(runner.getProcessor(), "Processor should instantiate without errors");
    }

    // ── Property validation ─────────────────────────────────────────────────────

    @Test
    public void testRequiredPropertiesNotSet() {
        // If no properties are set, the processor should refuse to start.
        // This is the "did you even read the docs?" test.
        runner.assertNotValid();
    }

    @Test
    public void testValidConfiguration() {
        configureRequiredProperties();
        runner.assertValid();
    }

    @Test
    public void testDefaultBatchSize() {
        configureRequiredProperties();
        assertEquals("100", runner.getProcessContext()
                .getProperty(PutZerobusIngest.BATCH_SIZE).getValue());
    }

    @Test
    public void testCustomBatchSize() {
        configureRequiredProperties();
        runner.setProperty(PutZerobusIngest.BATCH_SIZE, "500");
        runner.assertValid();
    }

    @Test
    public void testInvalidBatchSize() {
        configureRequiredProperties();
        // Negative batch size: because ingesting negative records would
        // require un-sending data, which is not yet a feature
        runner.setProperty(PutZerobusIngest.BATCH_SIZE, "-1");
        runner.assertNotValid();
    }

    @Test
    public void testDefaultWaitTimeout() {
        configureRequiredProperties();
        assertEquals("30 sec", runner.getProcessContext()
                .getProperty(PutZerobusIngest.WAIT_TIMEOUT).getValue());
    }

    @Test
    public void testCustomWaitTimeout() {
        configureRequiredProperties();
        runner.setProperty(PutZerobusIngest.WAIT_TIMEOUT, "60 sec");
        runner.assertValid();
    }

    @Test
    public void testDefaultMaxFlowFileSize() {
        configureRequiredProperties();
        assertEquals("1 MB", runner.getProcessContext()
                .getProperty(PutZerobusIngest.MAX_FLOWFILE_SIZE).getValue());
    }

    @Test
    public void testCustomMaxFlowFileSize() {
        configureRequiredProperties();
        runner.setProperty(PutZerobusIngest.MAX_FLOWFILE_SIZE, "5 MB");
        runner.assertValid();
    }

    // ── Relationships ───────────────────────────────────────────────────────────

    @Test
    public void testRelationships() {
        assertEquals(3, runner.getProcessor().getRelationships().size(),
                "Should have exactly 3 relationships");
        assertTrue(runner.getProcessor().getRelationships()
                .contains(PutZerobusIngest.REL_SUCCESS));
        assertTrue(runner.getProcessor().getRelationships()
                .contains(PutZerobusIngest.REL_FAILURE));
        assertTrue(runner.getProcessor().getRelationships()
                .contains(PutZerobusIngest.REL_RETRY));
    }

    // ── Sensitive properties ────────────────────────────────────────────────────

    @Test
    public void testSensitiveProperty() {
        // Client secret should be masked in the UI.
        // Because pasting credentials into a screenshot for a Jira ticket
        // is a security incident, not a bug report.
        assertTrue(PutZerobusIngest.CLIENT_SECRET.isSensitive());
    }

    // ── JSON structural validation ──────────────────────────────────────────────

    @Test
    public void testLooksLikeJson_validObject() {
        assertTrue(PutZerobusIngest.looksLikeJson("{\"key\": \"value\"}"));
    }

    @Test
    public void testLooksLikeJson_validArray() {
        assertTrue(PutZerobusIngest.looksLikeJson("[1, 2, 3]"));
    }

    @Test
    public void testLooksLikeJson_withWhitespace() {
        assertTrue(PutZerobusIngest.looksLikeJson("  { \"padded\": true }  \n"));
    }

    @Test
    public void testLooksLikeJson_nestedObject() {
        assertTrue(PutZerobusIngest.looksLikeJson("{\"outer\": {\"inner\": [1,2]}}"));
    }

    @Test
    public void testLooksLikeJson_null() {
        assertFalse(PutZerobusIngest.looksLikeJson(null));
    }

    @Test
    public void testLooksLikeJson_empty() {
        assertFalse(PutZerobusIngest.looksLikeJson(""));
    }

    @Test
    public void testLooksLikeJson_whitespaceOnly() {
        assertFalse(PutZerobusIngest.looksLikeJson("   \n\t  "));
    }

    @Test
    public void testLooksLikeJson_xml() {
        // XML is many things, but JSON is not one of them
        assertFalse(PutZerobusIngest.looksLikeJson("<root><value>42</value></root>"));
    }

    @Test
    public void testLooksLikeJson_csv() {
        assertFalse(PutZerobusIngest.looksLikeJson("name,age\nAlice,30"));
    }

    @Test
    public void testLooksLikeJson_plainText() {
        assertFalse(PutZerobusIngest.looksLikeJson("just a string"));
    }

    @Test
    public void testLooksLikeJson_mismatchedBraces() {
        // Starts like JSON, ends like... not JSON
        assertFalse(PutZerobusIngest.looksLikeJson("{\"key\": \"value\"]"));
    }

    // ── Delivery guarantee ──────────────────────────────────────────────────────

    @Test
    public void testDefaultDeliveryGuarantee() {
        configureRequiredProperties();
        runner.assertValid();
        assertEquals("guaranteed", runner.getProcessContext()
                .getProperty(PutZerobusIngest.DELIVERY_GUARANTEE).getValue());
    }

    @Test
    public void testBestEffortDeliveryGuarantee() {
        configureRequiredProperties();
        runner.setProperty(PutZerobusIngest.DELIVERY_GUARANTEE, "best-effort");
        runner.assertValid();
    }

    @Test
    public void testInvalidDeliveryGuarantee() {
        configureRequiredProperties();
        runner.setProperty(PutZerobusIngest.DELIVERY_GUARANTEE, "maybe");
        runner.assertNotValid();
    }

    // ── Chunking by size ────────────────────────────────────────────────────────

    @Test
    public void testChunkBySize_empty() {
        assertTrue(PutZerobusIngest.chunkBySize(Collections.emptyList(), 100).isEmpty());
    }

    @Test
    public void testChunkBySize_fitsInOneChunk() {
        final List<int[]> chunks = PutZerobusIngest.chunkBySize(List.of(10L, 10L, 10L), 100);
        assertEquals(1, chunks.size());
        assertEquals(0, chunks.get(0)[0]);
        assertEquals(3, chunks.get(0)[1]);
    }

    @Test
    public void testChunkBySize_splitsAndKeepsOrder() {
        // each record costs 40 + overhead, so two fit under 100 and a third does not
        final List<int[]> chunks = PutZerobusIngest.chunkBySize(List.of(40L, 40L, 40L, 40L, 40L), 100);
        assertEquals(3, chunks.size());
        assertEquals(0, chunks.get(0)[0]);
        assertEquals(2, chunks.get(0)[1]);
        assertEquals(2, chunks.get(1)[0]);
        assertEquals(4, chunks.get(1)[1]);
        assertEquals(4, chunks.get(2)[0]);
        assertEquals(5, chunks.get(2)[1]);
    }

    @Test
    public void testChunkBySize_oversizedRecordGetsItsOwnChunk() {
        final List<int[]> chunks = PutZerobusIngest.chunkBySize(List.of(10L, 500L, 10L), 100);
        assertEquals(3, chunks.size());
        assertEquals(1, chunks.get(1)[0]);
        assertEquals(2, chunks.get(1)[1]);
    }

    @Test
    public void testChunkBySize_staysUnderZerobusMessageLimit() {
        // 10 000 records of 1 KB is ~10 MB: too close to the 10 MB message limit for one batch
        final List<Long> sizes = new ArrayList<>(Collections.nCopies(10_000, 1_000L));
        final List<int[]> chunks = PutZerobusIngest.chunkBySize(sizes, PutZerobusIngest.MAX_BATCH_BYTES);
        assertEquals(2, chunks.size());
        for (int[] chunk : chunks) {
            assertTrue((chunk[1] - chunk[0]) * 1_000L < 10L * 1024 * 1024);
        }
        assertEquals(10_000, chunks.get(1)[1]);
    }

    // ── In-flight tracking ──────────────────────────────────────────────────────

    @Test
    public void testInflight_fitsUnderLimit() {
        final PutZerobusIngest.InflightTracker tracker = new PutZerobusIngest.InflightTracker();
        tracker.sent(1, 100);
        assertEquals(OptionalLong.empty(), tracker.offsetToWaitFor(100, 200));
        assertEquals(100, tracker.pendingRecords());
    }

    @Test
    public void testInflight_waitsForJustEnough() {
        final PutZerobusIngest.InflightTracker tracker = new PutZerobusIngest.InflightTracker();
        tracker.sent(1, 100);
        tracker.sent(2, 100);
        tracker.sent(3, 100);
        // 300 pending, 100 incoming, limit 300: acknowledging offset 1 is enough
        assertEquals(OptionalLong.of(1), tracker.offsetToWaitFor(100, 300));
        // limit 150: offsets 1 to 3 all have to go
        assertEquals(OptionalLong.of(3), tracker.offsetToWaitFor(100, 150));
    }

    @Test
    public void testInflight_chunkLargerThanLimitWaitsForEmptyBuffer() {
        final PutZerobusIngest.InflightTracker tracker = new PutZerobusIngest.InflightTracker();
        assertEquals(OptionalLong.empty(), tracker.offsetToWaitFor(500, 100));
        tracker.sent(7, 500);
        assertEquals(OptionalLong.of(7), tracker.offsetToWaitFor(500, 100));
    }

    @Test
    public void testInflight_ackIsCumulative() {
        final PutZerobusIngest.InflightTracker tracker = new PutZerobusIngest.InflightTracker();
        tracker.sent(1, 100);
        tracker.sent(2, 100);
        tracker.sent(3, 100);
        tracker.acked(2);
        assertEquals(100, tracker.pendingRecords());
        tracker.acked(2);
        assertEquals(100, tracker.pendingRecords());
        tracker.clear();
        assertEquals(0, tracker.pendingRecords());
    }

    // ── Helper ──────────────────────────────────────────────────────────────────

    private void configureRequiredProperties() {
        runner.setProperty(PutZerobusIngest.SERVER_ENDPOINT,
                "1234567890.zerobus.us-west-2.cloud.databricks.com");
        runner.setProperty(PutZerobusIngest.WORKSPACE_URL,
                "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com");
        runner.setProperty(PutZerobusIngest.TABLE_NAME,
                "main.default.security_events");
        runner.setProperty(PutZerobusIngest.CLIENT_ID, "test-client-id");
        runner.setProperty(PutZerobusIngest.CLIENT_SECRET, "test-client-secret");
    }
}
