package lib;

import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;

class DynamoDBIntegrationTest {

	private static DynamoDBProxyServer server;

	@BeforeAll
	public static void setupClass() throws Exception {
		System.setProperty("sqlite4java.library.path", "native-libs");
		String port = "8000";
		server = ServerRunner.createServerFromCommandLineArgs(new String[] { "-inMemory", "-port", port });
		server.start();
	}

	@AfterAll
	public static void teardownClass() throws Exception {
		server.stop();
	}

	@Test
	public void alwaysFail() {
		fail("Not implemented yet");
	}
}
