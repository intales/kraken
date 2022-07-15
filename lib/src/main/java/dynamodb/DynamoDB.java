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

	private Map<Interaction, UpdateData> accumulate(Map<Interaction, UpdateData> previous,
			Future<Vector<Interaction>> current, String type) {
		try {
			Vector<Interaction> interactions = current.get();
			interactions.stream().forEach(i -> {
				previous.computeIfPresent(i, (key, value) -> {
					value.increment(type);
					return value;
				});
				previous.computeIfAbsent(i, t -> new UpdateData(type));
			});
			return previous;
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return previous;
		}
	}

	private Map<Interaction, UpdateData> combine(Map<Interaction, UpdateData> first,
			Map<Interaction, UpdateData> second) {
		if (first.equals(second)) {
			System.err.println("EQUALS");
			return first;
		} else {
			System.err.println("NOT EQUALS");
			return pippo(first, second);
		}
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
				.reduce(initValue, (acc, elem) -> accumulate(acc, elem, tableConfiguration.getTypename()),
						(first, second) -> combine(first, second));
	}

	public static Map<Interaction, UpdateData> pippo(Map<Interaction, UpdateData> prev,
			Map<Interaction, UpdateData> curr) {
		for (Interaction key : curr.keySet()) {
			prev.computeIfPresent(key, (k, value) -> value.merge(curr.get(key)));
			prev.computeIfAbsent(key, t -> curr.get(key));
		}
		return prev;
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
				.reduce(initValue, DynamoDB::pippo);
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
}
