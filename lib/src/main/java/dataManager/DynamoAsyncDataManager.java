package dataManager;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
	List<InteractionContainer> data;
	Map<Interaction, InteractionData> map;

	public DynamoAsyncDataManager() {
		client = DynamoDbAsyncClient.builder()
				.region(Region.EU_CENTRAL_1).credentialsProvider(
						ProfileCredentialsProvider.create())
				.build();
		data = new ArrayList<>();
	}

	private void scan(String field, InteracionType type,
			String table) {
		Map<String, AttributeValue> mapStartKey = null;
		do {
			ScanRequest request = ScanRequest.builder()
					.tableName(table)
					.filterExpression("attribute_exists(toID) AND "
							+ field + " <> toID")
					.exclusiveStartKey(mapStartKey).build();
			CompletableFuture<ScanResponse> responseFuture = client
					.scan(request);
			ScanResponse response = responseFuture.join();
			mapStartKey = response.hasLastEvaluatedKey()
					? response.lastEvaluatedKey()
					: null;
			data.addAll(
					response.items().parallelStream().map(item -> {
						AttributeValue from = item.get(field),
								to = item.get("toID");
						Interaction interaction = new Interaction(
								from, to);
						InteractionData interactionData = new InteractionData();
						interactionData.increment(type);
						InteractionContainer interactionContainer = new InteractionContainer();
						interactionContainer.interaction = interaction;
						interactionContainer.interactionData = interactionData;
						return interactionContainer;
					}).toList());
		} while (mapStartKey != null);
	}

	private void update(Interaction key, InteractionData value) {
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
				.builder().tableName(interactionTableName).key(keyMap)
				.expressionAttributeValues(attributesMap)
				.updateExpression(expression).build();
		client.updateItem(updateRequestItem).join();
	}

	public void aggregate() {
		ExecutorService executor = Executors.newFixedThreadPool(32);
		map = data.parallelStream().reduce(
				new ConcurrentHashMap<Interaction, InteractionData>(),
				(hashMap, element) -> {
					hashMap.computeIfPresent(element.interaction,
							(key, val) -> {
								val.increment(
										element.interactionData);
								return val;
							});
					hashMap.computeIfAbsent(element.interaction,
							val -> {
								return element.interactionData;
							});
					return hashMap;
				}, (m1, m2) -> {
					m1.putAll(m2);
					return m1;
				});
		executor.submit(() -> map.entrySet().parallelStream()
				.forEach(t -> update(t.getKey(), t.getValue())));
		executor.shutdown();
		try {
			while (!executor.awaitTermination(1, TimeUnit.SECONDS))
				System.out.println("WAIT");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
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
		int l = 0, cm = 0, cl = 0;
		for (Entry<Interaction, InteractionData> entry : map
				.entrySet()) {
			InteractionData val = entry.getValue();
			l += val.getLikes();
			cm += val.getComments();
			cl += val.getCollaborations();
		}
		System.out.println("likes = " + l);
		System.out.println("comments = " + cm);
		System.out.println("collaborations = " + cl);

		System.out.println("Total aggregated items = " + map.size());
	}

	private class InteractionContainer {
		public Interaction interaction;
		public InteractionData interactionData;
	}
}
