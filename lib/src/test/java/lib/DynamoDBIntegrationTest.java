package lib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import config.Configuration;
import config.TableConfiguration;
import dynamodb.DynamoDB;
import main.DataManager;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamoDBIntegrationTest {

	private static LocalDynamoDb server;

	@BeforeAll
	public static void setupClass() throws Exception {
		server = new LocalDynamoDb();
		start();
	}

	private static Configuration getConfiguration() {
		List<TableConfiguration> tableConfigurations = Arrays
				.asList(new TableConfiguration("Like", "fromID", 1, "likes", ":lik", null, 1, 1),
						new TableConfiguration("Comments", "fromID", 1, "comm", ":com", null, 1, 1));
		Configuration configuration = new Configuration(tableConfigurations, 2, "Interactions", ":aff", "affinity");
		return configuration;
	}

	private static void start() {
		server.start();
	}

	@AfterAll
	public static void teardownClass() {
		server.stop();
	}

	private String createTable(String tableName) {
		return server.createTable(tableName, "id");
	}

	private boolean insertData(String tableName, Map<String, AttributeValue> itemValues) {
		return server.insertData(tableName, itemValues);
	}

	private static int randomInts(int min, int max, int exept) {
		Random random = new Random();
		int result = random.ints(min, max).findFirst().getAsInt();
		if (result == exept) {
			if (random.doubles().findFirst().getAsDouble() < .5)
				result--;
			else
				result++;
		}
		return result;
	}

	public void populateTable(String tableName) {
		int start = 0, end = 10;
		assertEquals(tableName, createTable(tableName));

		Map<String, AttributeValue> itemValues = new HashMap<>();
		// insert dummy data
		List<Integer> range = IntStream.range(start, end).boxed().collect(Collectors.toList());
		for (Integer i : range) {
			itemValues.put("id", AttributeValue.fromS("ID-" + i));
			itemValues.put("fromID", AttributeValue.fromS("USER-" + i));
			itemValues.put("toID", AttributeValue.fromS("USER-" + randomInts(start, end, i)));
			assertTrue(insertData(tableName, itemValues));
		}
	}

	@Test
	public void scan() {
		populateTable("Like");
		populateTable("Comment");
		DataManager dynamodb = new DynamoDB(getConfiguration(), server.getClient());
		dynamodb.scan();
	}
}
