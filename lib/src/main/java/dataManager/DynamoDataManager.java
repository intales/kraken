package dataManager;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import affinity.InteracionType;
import affinity.Interaction;
import affinity.InteractionData;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public class DynamoDataManager implements DataManager {
	int numberOfThreads = 8;
	String interactionTableName;
	Region region;
	AwsCredentialsProvider credentialsProvider;
	ExecutorService executor;
	DynamoDbClient client;
	ConcurrentHashMap<Interaction, InteractionData> map;
	LinkedBlockingQueue<UpdateItemRequest> updateQueue;
	Vector<Integer> counterVector;

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
		updateQueue = new LinkedBlockingQueue<>();
		counterVector = new Vector<>();

		client = DynamoDbClient.builder()
				.credentialsProvider(credentialsProvider)
				.region(region).build();
	}

	public void shutdown() {
		executor.shutdown();
		try {
			while (!executor.awaitTermination(10, TimeUnit.SECONDS))
				System.out.println("Not terminated");
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			System.out.println("Terminated");
		}
	}

	public void updateInteraction() {
		numberOfThreads = 2 << 5;
		executor = Executors.newFixedThreadPool(numberOfThreads);
		List<UpdateTask> taskArray = new ArrayList<>();
		for (int thread = 0; thread < numberOfThreads; thread++) {
			// Runnable task that will only scan one segment
			UpdateTask task = new UpdateTask(updateQueue, client,
					counterVector);
			// Execute the task
			executor.execute(task);
			// Add to the array of task
			taskArray.add(task);
		}

		for (Entry<Interaction, InteractionData> entry : map
				.entrySet()) {
			Interaction key = entry.getKey();
			InteractionData val = entry.getValue();

			Map<String, AttributeValue> keyMap = key.getMap();

			Map<String, AttributeValue> attributesMap = new HashMap<>();

			// createdAt
			attributesMap.put(":c", AttributeValue
					.fromS(new Date().toInstant().toString()));
			// updatedAt
			attributesMap.put(":u", AttributeValue
					.fromS(new Date().toInstant().toString()));
			attributesMap.putAll(val.getMap());

			String expression = "SET updatedAt = :u, createdAt = if_not_exists(createdAt, :c), ";
			expression += "likes = :l, comments = :cm, collaborations = :cl";
			UpdateItemRequest updateRequestItem = UpdateItemRequest
					.builder().tableName(interactionTableName)
					.key(keyMap)
					.expressionAttributeValues(attributesMap)
					.updateExpression(expression).build();
			updateQueue.add(updateRequestItem);
		}
		System.out.println("All elements added to queue");
		for (UpdateTask updateTask : taskArray) {
			updateTask.stop();
		}
	}

	public void count() {
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
		System.out.println(
				"count vector size = " + counterVector.size());
		Integer c = counterVector.stream().mapToInt(i -> i).sum();
		System.out.println("Total updates = " + c);
		System.out.println("Total aggregated items = " + map.size());
		if (map.size() != c)
			System.err.println(
					"Mismatch between the updated done and the aggregated data");
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
