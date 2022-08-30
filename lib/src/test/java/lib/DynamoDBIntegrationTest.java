package lib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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

	private static final String TO_ID = "toID";
	private static final String FROM_ID = "fromID";
	private static final String TABLE_INTERACTION = "Interactions";
	private static final String TABLE_COLLABORAION = "Collaboration";
	private static final String TABLE_COMMENT = "Comment";
	private static final String TABLE_LIKE = "Like";
	private static LocalDynamoDb server;

	@BeforeAll
	public static void setupClass() throws Exception {
		server = new LocalDynamoDb();
		start();
	}

	private static Configuration getConfiguration() {
		List<TableConfiguration> tableConfigurations = Arrays
				.asList(TableConfiguration
						.builder()
						.withName(TABLE_LIKE)
						.withField(FROM_ID)
						.withThreads(1)
						.withTypename("likes")
						.withKey(":lik")
						.withWeight(1)
						.withExponent(1)
						.build(),
						TableConfiguration
								.builder()
								.withName(TABLE_COMMENT)
								.withField(FROM_ID)
								.withThreads(1)
								.withTypename("comments")
								.withKey(":com")
								.withWeight(1)
								.withExponent(1)
								.build(),
						TableConfiguration
								.builder()
								.withName(TABLE_COLLABORAION)
								.withField(FROM_ID)
								.withThreads(1)
								.withTypename("collaborations")
								.withKey(":col")
								.withWeight(1)
								.withExponent(1)
								.build());
		Configuration configuration = Configuration
				.builder()
				.withUpdateTable(TABLE_INTERACTION)
				.withAffinityField("affinity")
				.withAffinityKey(":aff")
				.withTableConfigurations(tableConfigurations)
				.build();

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

	private String createTable(String tableName, String partitionKey, String sortKey) {
		return server.createTable(tableName, partitionKey, sortKey);
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
			itemValues.put(FROM_ID, AttributeValue.fromS("USER-" + i));
			itemValues.put(TO_ID, AttributeValue.fromS("USER-" + randomInts(start, end, i)));
			assertTrue(insertData(tableName, itemValues));
		}
	}

	@Test
	public void scan() {
		populateTable(TABLE_LIKE);
		populateTable(TABLE_COMMENT);
		populateTable(TABLE_COLLABORAION);
		createTable(TABLE_INTERACTION, FROM_ID, TO_ID);
		DataManager dynamodb = new DynamoDB(getConfiguration(), server.getClient());
		dynamodb.scan();
		dynamodb.update();
		assertNotEquals(server.scanTable(TABLE_INTERACTION), 0);
	}
}
