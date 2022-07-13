package dataManager;

import java.util.ArrayList;
import java.util.Collections;
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
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public class DynamoDataManager implements DataManager {
	ExecutorService executor = null;
	String interactionTableName;
	Region region;
	AwsCredentialsProvider credentialsProvider;
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
		map = new ConcurrentHashMap<>();
		updateQueue = new LinkedBlockingQueue<>();
		counterVector = new Vector<>();
		client = DynamoDbClient.builder()
				.credentialsProvider(credentialsProvider)
				.region(region).build();
	}

	private ExecutorService initThreadPool(int numberOfThreads) {
		return Executors.newFixedThreadPool(numberOfThreads);
	}

	public void shutdown() {
		executor.shutdown();
		try {
			while (!executor.awaitTermination(1000,
					TimeUnit.MILLISECONDS))
				System.out.println("Not terminated");
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			System.out.println("Terminated");
		}
	}

	public void updateInteraction() {
		int numberOfThreads = 2 << 5;
		executor = initThreadPool(numberOfThreads);
		ArrayList<Interaction> list = Collections.list(map.keys());
		for (int thread = 0; thread < numberOfThreads; thread++) {
			// Runnable task that will only scan one segment
			UpdateTask task = new UpdateTask(list, client,
					counterVector, thread, numberOfThreads, map,
					interactionTableName);
			// Execute the task
			executor.execute(task);
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
		addThreads(likesTableName, field, InteracionType.like, 8);
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

	@SuppressWarnings("unused")
	private void addThreads(String tableName, String field,
			InteracionType type) {
		addThreads(tableName, field, type, 1);
	};

	private void addThreads(String tableName, String field,
			InteracionType type, int numberOfThreads) {
		if (executor == null)
			executor = initThreadPool(numberOfThreads);
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
