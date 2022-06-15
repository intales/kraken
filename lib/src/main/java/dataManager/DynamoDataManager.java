package dataManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import affinity.InteracionType;
import affinity.Interaction;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

public class DynamoDataManager implements DataManager {
	String interactionTableName;
	Region region;
	AwsCredentialsProvider credentialsProvider;
	int numberOfThreads = 2;
	ExecutorService executor;
	ConcurrentHashMap<Interaction, InteracionType> map;
	DynamoDbClient client;
	DynamoDbAsyncClient clientAsync;
	ConcurrentHashMap<Interaction, InteractionData> reduce;

	public DynamoDataManager() {
		this(Region.EU_CENTRAL_1,
				"Interaction-lgwgf74xczh3jddacdgxrh7smy-test");
	}
	public DynamoDataManager(Region region,
			String interactionTableName) {
		this.region = region;
		this.interactionTableName = interactionTableName;
		credentialsProvider = ProfileCredentialsProvider.create();
		executor = Executors.newFixedThreadPool(numberOfThreads);
		map = new ConcurrentHashMap<>();
		reduce = new ConcurrentHashMap<>();

		client = DynamoDbClient.builder()
				.credentialsProvider(credentialsProvider)
				.region(region).build();

		clientAsync = DynamoDbAsyncClient.builder()
				.credentialsProvider(credentialsProvider)
				.region(region).build();
	}

	public void aggregate() {
		UpdateItemRequest.Builder requestBuilder = UpdateItemRequest
				.builder();
		AttributeValue.Builder attributeValueBuilder = AttributeValue
				.builder();
		Map<String, AttributeValue> keysMap = new HashMap<>();
		ArrayList<CompletableFuture<UpdateItemResponse>> responses = new ArrayList<>();
		for (Entry<Interaction, InteracionType> entry : map
				.entrySet()) {
			Interaction key = entry.getKey();
			InteracionType val = entry.getValue();
			String expression = "ADD";
			Map<String, AttributeValue> attributesMap = new HashMap<>();
			keysMap.put("fromID",
					attributeValueBuilder.s(key.fromID).build());
			keysMap.put("toID",
					attributeValueBuilder.s(key.toID).build());
			String attributeKey;
			AttributeValue attributeValue = AttributeValue.fromN("1");
			switch (val) {
				case like : {
					attributeKey = ":l";
					expression += " likes :l";
					break;
				}
				case comment : {
					attributeKey = ":cm";
					expression += " comments :cm";
					break;
				}
				case collaboration : {
					attributeKey = ":cl";
					expression += " collaborations :cl";
					break;
				}
				default :
					throw new IllegalArgumentException(
							"Unexpected value: " + val);
			}
			attributesMap.put(attributeKey, attributeValue);
			UpdateItemRequest request = requestBuilder
					.tableName(interactionTableName).key(keysMap)
					.updateExpression(expression)
					.expressionAttributeValues(attributesMap).build();
			// UpdateItemResponse response = client.updateItem(request);

			CompletableFuture<UpdateItemResponse> response = clientAsync
					.updateItem(request);
			responses.add(response);
		}
		System.out.println("Total response: " + responses.size());
		for (CompletableFuture<UpdateItemResponse> response : responses) {
			try {
				response.get();
				// System.out.println();
			} catch (Exception e) {
				e.printStackTrace();
				return;
			}
		}
		System.out.println("Aggregation over");
	}

	public void shutdown() {
		executor.shutdown();
		try {
			while (!executor.awaitTermination(1, TimeUnit.SECONDS))
				System.out.println("Not terminated");
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			System.out.println("Terminated");
			System.out.println("Total entries: " + map.size());
		}
	}

	@Override
	public DataManager getLikes() {
		String likesTableName = "Like-lgwgf74xczh3jddacdgxrh7smy-test";
		String field = "likeUserId";
		addThreads(likesTableName, field, InteracionType.like);
		return this;
	}

	@Override
	public DataManager getComments() {
		String commentTableName = "Comment-lgwgf74xczh3jddacdgxrh7smy-test";
		String field = "commentUserId";
		addThreads(commentTableName, field, InteracionType.comment);
		return this;
	}

	@Override
	public DataManager getCollaborations() {
		String collaborationsTableName = "Collab-lgwgf74xczh3jddacdgxrh7smy-test";
		String field = "userID";
		addThreads(collaborationsTableName, field,
				InteracionType.collaboration);
		return this;
	}

	private void addThreads(String tableName, String field,
			InteracionType type) {
		for (int thread = 0; thread < numberOfThreads; thread++) {
			// Runnable task that will only scan one segment
			ScanSegmentTask task = new ScanSegmentTask(tableName,
					numberOfThreads, thread, client, field, map,
					type);
			// Execute the task
			executor.execute(task);
		}
	}
}
