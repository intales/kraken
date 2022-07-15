package dynamodb;

import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import config.Configuration;
import config.TableConfiguration;
import main.DataManager;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class DynamoDB implements DataManager {
	Configuration configuration;
	Region region;
	AwsCredentialsProvider credentialsProvider;
	DynamoDbClient client;
	ExecutorService executor = null;
	Vector<Integer> counterVector;

	public DynamoDB(Configuration configuration) {
		this(configuration, Region.EU_CENTRAL_1);
	}

	public DynamoDB(Configuration configuration, Region region) {
		this.region = region;
		this.configuration = configuration;
		credentialsProvider = ProfileCredentialsProvider.create();
		counterVector = new Vector<>();
		client = DynamoDbClient.builder().credentialsProvider(credentialsProvider).region(region).build();
	}

	private Map<Interaction, UpdateData> scanTable(TableConfiguration tableConfiguration) {
		// in order to scan a dynamodb table
		// the table name, field, type and threads number must be retrieved
		int total = tableConfiguration.getThreads();
		Vector<Future<Vector<Interaction>>> tasksList = new Vector<>();
		for (int i = 0; i < total; i++) {
			ScanSegmentTask scanSegmentTask = new ScanSegmentTask(client, i, total, tableConfiguration.getName(),
					tableConfiguration.getField());
			Future<Vector<Interaction>> future = executor.submit(scanSegmentTask);
			tasksList.add(future);
		}
		Map<Interaction, UpdateData> initValue = new ConcurrentHashMap<>();
		return tasksList
				.stream()
				.parallel()
				.map(DynamoDB::handleFuture)
				.reduce(initValue,
						(accumulator, current) -> accumulate(accumulator, current, tableConfiguration.getTypename()),
						DynamoDB::combineMaps);
	}

	@Override
	public void scan() {
		executor = initThreadPool();
		Map<Interaction, UpdateData> initValue = new ConcurrentHashMap<>();
		Map<Interaction, UpdateData> result = configuration
				.getTableConfigurations()
				.stream()
				.parallel()
				.map(table -> scanTable(table))
				.reduce(initValue, DynamoDB::combineMaps);
		System.out.println("total = " + result.size());
		shutdown();
	}

	@Override
	public void update() {
		// TODO Auto-generated method stub

	}

	private ExecutorService initThreadPool() {
		return Executors.newCachedThreadPool();
	}

	public void shutdown() {
		executor.shutdown();
		try {
			while (!executor.awaitTermination(1000, TimeUnit.MILLISECONDS))
				System.out.println("Not terminated");
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			System.out.println("Terminated");
		}
	}

	private static Map<Interaction, UpdateData> accumulate(Map<Interaction, UpdateData> previous,
			Vector<Interaction> current, String type) {
		if (current == null)
			return previous;
		current.stream().forEach(interaction -> {
			previous.merge(interaction, new UpdateData(type), UpdateData::merge);
		});
		return previous;
	}

	private static <T> T handleFuture(Future<T> future) {
		try {
			return future.get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			return null;
		}
	}

	public static <T> Map<T, UpdateData> combineMaps(Map<T, UpdateData> prev, Map<T, UpdateData> curr) {
		if (prev.equals(curr))
			return prev;
		curr.forEach((key, value) -> prev.merge(key, value, UpdateData::merge));
		return prev;
	}
}
