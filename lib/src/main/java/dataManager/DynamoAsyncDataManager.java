package dataManager;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import affinity.InteracionType;
import affinity.Interaction;
import affinity.InteractionData;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public class DynamoAsyncDataManager implements DataManager {
	private static final String interactionTableName = "Interaction-lgwgf74xczh3jddacdgxrh7smy-test";
	DynamoDbAsyncClient client;
	ConcurrentHashMap<Interaction, InteractionData> map;

	public DynamoAsyncDataManager() {
		client = DynamoDbAsyncClient.builder()
				.region(Region.EU_CENTRAL_1).credentialsProvider(
						ProfileCredentialsProvider.create())
				.build();
		map = new ConcurrentHashMap<>();
	}

	private void scan(String field, InteracionType type,
			String table) {
		Map<String, AttributeValue> mapStartKey = null;
		do {
			ScanRequest request = ScanRequest.builder()
					.tableName(table)
					.filterExpression("attribute_exists(toID)")
					.exclusiveStartKey(mapStartKey).build();
			CompletableFuture<ScanResponse> responseFuture = client
					.scan(request);
			ScanResponse response = responseFuture.join();
			mapStartKey = response.hasLastEvaluatedKey()
					? response.lastEvaluatedKey()
					: null;
			System.out.println("retrived " + response.items().size());
			response.items().parallelStream().forEach(item -> {
				AttributeValue from = item.get(field),
						to = item.get("toID");
				if (from.equals(to))
					return;
				Interaction interaction = new Interaction(from, to);
				map.compute(interaction, (key, value) -> {
					if (value == null) {
						value = new InteractionData();
					}
					value.increment(type);
					return value;
				});
				return;
			});
		} while (mapStartKey != null);
	}

	public void aggregate() {
		map.entrySet().parallelStream().forEach(item -> {
			Interaction key = item.getKey();
			InteractionData value = item.getValue();
			Map<String, AttributeValue> keyMap = key.getMap();

			Map<String, AttributeValue> attributesMap = new HashMap<>();
			// createdAt
			attributesMap.put(":c", AttributeValue
					.fromS(new Date().toInstant().toString()));
			// updatedAt
			attributesMap.put(":u", AttributeValue
					.fromS(new Date().toInstant().toString()));
			attributesMap.putAll(value.getMap());

			String expression = "SET updatedAt = :u, createdAt = if_not_exists(createdAt, :c), ";
			expression += "likes = :l, comments = :cm, collaborations = :cl";
			UpdateItemRequest updateRequestItem = UpdateItemRequest
					.builder().tableName(interactionTableName)
					.key(keyMap)
					.expressionAttributeValues(attributesMap)
					.updateExpression(expression).build();
			try {
				client.updateItem(updateRequestItem).get();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
	}

	@Override
	public DataManager getLikes() {
		InteracionType type = InteracionType.like;
		String field = "likeUserId";
		String table = "Like-lgwgf74xczh3jddacdgxrh7smy-test";
		scan(field, type, table);
		return this;
	}

	@Override
	public DataManager getComments() {
		InteracionType type = InteracionType.comment;
		String field = "commentUserId";
		String table = "Comment-lgwgf74xczh3jddacdgxrh7smy-test";
		scan(field, type, table);
		return this;
	}

	@Override
	public DataManager getCollaborations() {
		InteracionType type = InteracionType.collaboration;
		String field = "userID";
		String table = "Collab-lgwgf74xczh3jddacdgxrh7smy-test";
		scan(field, type, table);
		return this;
	}

	public static void main(String[] args) {
		DynamoAsyncDataManager db = new DynamoAsyncDataManager();
		Instant start = Instant.now();
		db.getLikes().getComments().getCollaborations();
		db.aggregate();
		Instant finish = Instant.now();
		System.out.println("Time = "
				+ Duration.between(start, finish).toMillis() + " ms");
		db.count();
	}

	private void count() {
		System.out.println("Total aggregated = " + map.size());
	}
}
