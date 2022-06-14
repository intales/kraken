package dataManager;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import affinity.InteracionType;
import affinity.Interaction;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class DynamoDataManager implements DataManager {
	String interactionTableName;
	Region region;
	AwsCredentialsProvider credentialsProvider;
	int numberOfThreads = 4;
	DynamoDbClient client;
	ExecutorService executor;
	ConcurrentHashMap<Interaction, InteracionType> map;

	public DynamoDataManager() {
		this(Region.EU_CENTRAL_1,
				"Interaction-lgwgf74xczh3jddacdgxrh7smy-test");
	}
	public DynamoDataManager(Region region,
			String interactionTableName) {
		this.region = region;
		this.interactionTableName = interactionTableName;
		credentialsProvider = ProfileCredentialsProvider.create();
		client = DynamoDbClient.builder()
				.credentialsProvider(credentialsProvider)
				.region(region).build();
		executor = Executors.newFixedThreadPool(numberOfThreads);
		map = new ConcurrentHashMap<>(1024);
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
			System.out.println(map.size());
			for (Entry<Interaction, InteracionType> entry : map
					.entrySet()) {
				System.out.println(entry.getKey().toString() + " -> "
						+ entry.getValue());
			}
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
