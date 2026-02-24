package la.dere.nifi.zerobus;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for PutZerobusIngest processor.
 *
 * These tests validate processor configuration and property handling.
 * Integration tests against a live Databricks workspace should use
 * the IT profile: mvn verify -Pit
 */
public class PutZerobusIngestTest {

    private TestRunner runner;

    @Before
    public void setUp() {
        runner = TestRunners.newTestRunner(PutZerobusIngest.class);
    }

    @Test
    public void testProcessorLoads() {
        assertNotNull(runner.getProcessor());
    }

    @Test
    public void testRequiredPropertiesNotSet() {
        runner.assertNotValid();
    }

    @Test
    public void testValidConfiguration() {
        runner.setProperty(PutZerobusIngest.SERVER_ENDPOINT,
                "1234567890.zerobus.us-west-2.cloud.databricks.com");
        runner.setProperty(PutZerobusIngest.WORKSPACE_URL,
                "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com");
        runner.setProperty(PutZerobusIngest.TABLE_NAME,
                "main.default.security_events");
        runner.setProperty(PutZerobusIngest.CLIENT_ID,
                "test-client-id");
        runner.setProperty(PutZerobusIngest.CLIENT_SECRET,
                "test-client-secret");
        runner.assertValid();
    }

    @Test
    public void testDefaultBatchSize() {
        runner.setProperty(PutZerobusIngest.SERVER_ENDPOINT,
                "1234567890.zerobus.us-west-2.cloud.databricks.com");
        runner.setProperty(PutZerobusIngest.WORKSPACE_URL,
                "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com");
        runner.setProperty(PutZerobusIngest.TABLE_NAME,
                "main.default.events");
        runner.setProperty(PutZerobusIngest.CLIENT_ID, "id");
        runner.setProperty(PutZerobusIngest.CLIENT_SECRET, "secret");

        assertEquals("100", runner.getProcessContext()
                .getProperty(PutZerobusIngest.BATCH_SIZE).getValue());
    }

    @Test
    public void testRelationships() {
        assertEquals(3, runner.getProcessor().getRelationships().size());
        assertTrue(runner.getProcessor().getRelationships()
                .contains(PutZerobusIngest.REL_SUCCESS));
        assertTrue(runner.getProcessor().getRelationships()
                .contains(PutZerobusIngest.REL_FAILURE));
        assertTrue(runner.getProcessor().getRelationships()
                .contains(PutZerobusIngest.REL_RETRY));
    }

    @Test
    public void testCustomBatchSize() {
        runner.setProperty(PutZerobusIngest.SERVER_ENDPOINT,
                "1234567890.zerobus.us-west-2.cloud.databricks.com");
        runner.setProperty(PutZerobusIngest.WORKSPACE_URL,
                "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com");
        runner.setProperty(PutZerobusIngest.TABLE_NAME,
                "main.default.events");
        runner.setProperty(PutZerobusIngest.CLIENT_ID, "id");
        runner.setProperty(PutZerobusIngest.CLIENT_SECRET, "secret");
        runner.setProperty(PutZerobusIngest.BATCH_SIZE, "500");
        runner.assertValid();
    }

    @Test
    public void testInvalidBatchSize() {
        runner.setProperty(PutZerobusIngest.SERVER_ENDPOINT,
                "1234567890.zerobus.us-west-2.cloud.databricks.com");
        runner.setProperty(PutZerobusIngest.WORKSPACE_URL,
                "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com");
        runner.setProperty(PutZerobusIngest.TABLE_NAME,
                "main.default.events");
        runner.setProperty(PutZerobusIngest.CLIENT_ID, "id");
        runner.setProperty(PutZerobusIngest.CLIENT_SECRET, "secret");
        runner.setProperty(PutZerobusIngest.BATCH_SIZE, "-1");
        runner.assertNotValid();
    }

    @Test
    public void testSensitiveProperty() {
        // Client secret should be marked as sensitive
        assertTrue(PutZerobusIngest.CLIENT_SECRET.isSensitive());
    }
}
