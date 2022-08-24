package lib;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DynamoDBIntegrationTest {

	private static LocalDynamoDb server;

	@BeforeAll
	public static void setupClass() throws Exception {
		server = new LocalDynamoDb();
		start();
	}

	private static void start() {
		server.start();
	}

	@AfterAll
	public static void teardownClass() {
		server.stop();
	}

	@Test
	public void alwaysFail() {
		System.out.println(server.createTable("Like", "id"));
		return;
	}
}
