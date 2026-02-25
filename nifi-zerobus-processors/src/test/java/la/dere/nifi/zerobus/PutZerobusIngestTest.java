package la.dere.nifi.zerobus;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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

    @Before
    public void setUp() {
        runner = TestRunners.newTestRunner(PutZerobusIngest.class);
    }

    // ── Processor loading ───────────────────────────────────────────────────────

    @Test
    public void testProcessorLoads() {
        assertNotNull("Processor should instantiate without errors", runner.getProcessor());
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
        assertEquals("Should have exactly 3 relationships", 3,
                runner.getProcessor().getRelationships().size());
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
