package lib.dynamodb;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import config.Configuration;
import config.TableConfiguration;
import dynamodb.UpdateData;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class UpdateDataTest {

	@Test
	@DisplayName("Construct Update Data without any parameter")
	void testUpdateData() {
		UpdateData updateData;
		updateData = new UpdateData();
		assertNotNull(updateData.getMap());
	}

	@Test
	@DisplayName("Construct Update Data with a key")
	void testUpdateDataString() {
		UpdateData updateData;
		String key = ":lik";
		String value = "1.0";
		updateData = new UpdateData(key);
		assertNotNull(updateData.getMap());
		assertTrue(updateData.getMap().containsKey(key));
		assertEquals(updateData.getMap().get(key), AttributeValue.fromN(value));
	}

	@Test
	@DisplayName("Increment by 1.0 a specific key")
	void testIncrement() {
		UpdateData updateData;
		String key = ":lik";
		String value = "2.0";
		updateData = new UpdateData(key);
		updateData.increment(key);
		assertEquals(updateData.getMap().get(key), AttributeValue.fromN(value));
	}

	@Test
	@DisplayName("Add a double n to specific key")
	void testAdd() {
		UpdateData updateData;
		String key = ":lik", otherKey = ":com";
		String value = "3.0";
		double increment = 2.0;
		updateData = new UpdateData(key);
		updateData.add(key, increment);
		assertEquals(updateData.getMap().get(key), AttributeValue.fromN(value));
		updateData.add(otherKey, increment);
		assertEquals(updateData.getMap().get(otherKey), AttributeValue.fromN(String.valueOf(increment)));
	}

	@Test
	@DisplayName("Merge two update data")
	void testMerge() {
		UpdateData firstUpdateData, secondUpdateData, mergedUpdateData;
		firstUpdateData = new UpdateData(":lik");
		secondUpdateData = new UpdateData(":lik");
		secondUpdateData.increment(":com");
		mergedUpdateData = new UpdateData(":com");
		mergedUpdateData.add(":lik", 2.0);

		assertEquals(UpdateData.merge(firstUpdateData, secondUpdateData).getMap(), mergedUpdateData.getMap());
	}

	@Test
	@DisplayName("Return a valid DynamoDB update expression")
	void testGetUpdateExpression() {
		String key = ":lik", separator = " ";
		Map<String, String> keyTypeMap = new HashMap<>();
		UpdateData updateData = new UpdateData(key);
		keyTypeMap.put(key, "like");
		String expression = updateData.getUpdateExpression(keyTypeMap, separator);

		assertAll("Check correctness of getUpdateExpression",
				// not null
				() -> assertNotNull(expression),
				// assert that last char is not a comma
				() -> assertNotEquals(expression.trim().lastIndexOf(","), expression.trim().length() - 1),
				() -> assertThrows(IllegalArgumentException.class,
						() -> updateData.getUpdateExpression(null, separator)),
				() -> assertThrows(IllegalArgumentException.class,
						() -> updateData.getUpdateExpression(keyTypeMap, null)),
				() -> assertThrows(IllegalArgumentException.class,
						() -> updateData.getUpdateExpression(new HashMap<>(), separator)));
	}

	@Test
	@DisplayName("Calculare correct affinity according to Configuration")
	void testComputeAffinityConfiguration() {
		String key = ":lik";
		UpdateData updateData = new UpdateData(key);
		Double expectedAffinity = 1.2;
		String[] operations = { "increment" };
		Configuration configuration = Configuration
				.builder()
				.withTableConfigurations(Arrays
						.asList(TableConfiguration
								.builder()
								.withName("Like")
								.withField("fromID")
								.withThreads(1)
								.withTypename("like")
								.withKey(":lik")
								.withAffinityOperations(operations)
								.withExponent(2)
								.withWeight(0.3)
								.build()))
				.build();
		Double affinity = updateData.computeAffinity(configuration);
		assertEquals(affinity, expectedAffinity);
	}

	@Test
	@DisplayName("Calculare correct affinity according to Configuration and passed table")
	void testComputeAffinityMapOfStringDoubleConfiguration() {
		Double expectedAffinity = 38.3;
		String[] operations = { "increment" };
		String affinityKey = ":aff";
		Map<String, Double> map = new HashMap<>();
		map.put(":lik", 10.0);
		map.put(":com", 2.0);
		Configuration configuration = Configuration
				.builder()
				.withAffinityKey(affinityKey)
				.withTableConfigurations(Arrays
						.asList(TableConfiguration
								.builder()
								.withName("Like")
								.withField("fromID")
								.withThreads(1)
								.withTypename("likes")
								.withKey(":lik")
								.withAffinityOperations(operations)
								.withExponent(2)
								.withWeight(0.3)
								.build(),
								TableConfiguration
										.builder()
										.withName("Comment")
										.withField("fromID")
										.withThreads(1)
										.withTypename("comments")
										.withKey(":com")
										.withExponent(1)
										.withWeight(1)
										.build()))
				.build();
		Map<String, AttributeValue> affinityMap = UpdateData.computeAffinity(map, configuration);
		assertEquals(affinityMap.get(affinityKey).n(), expectedAffinity.toString());
	}

	@Test
	@DisplayName("Test custom sigmoid")
	void testCustomSigmoid() {
		double delta = 0.1;
		assertAll("Custom sigmoid correctness", () -> assertEquals(UpdateData.customSigmoid(0), 0.0),
				() -> assertEquals(UpdateData.customSigmoid(20), 22.8, delta),
				() -> assertEquals(UpdateData.customSigmoid(40), 43.4, delta),
				() -> assertEquals(UpdateData.customSigmoid(60), 60.2, delta),
				() -> assertEquals(UpdateData.customSigmoid(80), 73.0, delta),
				() -> assertEquals(UpdateData.customSigmoid(100), 82.1, delta));
	}
}
