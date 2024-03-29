package dynamodb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import config.Configuration;
import config.TableConfiguration;
import main.DataManager;
import main.TicToc;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class DynamoDB implements DataManager {
	private Configuration configuration;
	private DynamoDbClient client;
	private ExecutorService executor = null;
	private Vector<Integer> counterVector = new Vector<>();
	private Map<Interaction, UpdateData> data = null;
	private boolean dryRun;

	public DynamoDB(Configuration configuration, ProfileCredentialsProvider credentialsProvider, boolean dryRun) {
		this(configuration, Region.EU_CENTRAL_1, credentialsProvider, dryRun);
	}

	public DynamoDB(Configuration configuration, Region region, ProfileCredentialsProvider credentialsProvider,
			boolean dryRun) {
		this.configuration = configuration;
		this.dryRun = dryRun;
		client = DynamoDbClient.builder().credentialsProvider(credentialsProvider).region(region).build();
	}

	public DynamoDB(Configuration configuration, boolean dryRun) {
		this.configuration = configuration;
		this.dryRun = dryRun;
		client = DynamoDbClient.builder().build();
	}

	public DynamoDB(Configuration configuration, DynamoDbClient client) {
		this.configuration = configuration;
		this.dryRun = false;
		this.client = client;
	}

	private Map<Interaction, UpdateData> scanTable(TableConfiguration tableConfiguration, Optional<String> startDate,
			Optional<String> endDate) {
		// in order to scan a dynamodb table
		// the table name, field, type and threads number must be retrieved
		int totalThreads = tableConfiguration.getThreads();
		Vector<Future<Vector<Interaction>>> tasksList = new Vector<>();
		for (int thread = 0; thread < totalThreads; thread++) {
			ScanSegmentTask scanSegmentTask = new ScanSegmentTask(client, thread, totalThreads,
					tableConfiguration.getName(), tableConfiguration.getField(), startDate, endDate);
			Future<Vector<Interaction>> future = executor.submit(scanSegmentTask);
			tasksList.add(future);
		}
		Map<Interaction, UpdateData> initValue = new ConcurrentHashMap<>();
		return tasksList
				.stream()
				.parallel()
				.map(DynamoDB::handleFuture)
				.reduce(initValue,
						(accumulator, current) -> accumulate(accumulator, current, tableConfiguration.getKey()),
						DynamoDB::combineMaps);
	}

	private void _scan(Optional<String> startDate, Optional<String> endDate) {
		executor = initThreadPool();
		Map<Interaction, UpdateData> initValue = new ConcurrentHashMap<>();
		data = configuration
				.getTableConfigurations()
				.stream()
				.parallel()
				.map(table -> scanTable(table, startDate, endDate))
				.reduce(initValue, DynamoDB::combineMaps);
		shutdown();
		System.out.println("Total aggregated data = " + data.size());
	}

	@Override
	public void scan() {
		Optional<String> empty = Optional.empty();
		_scan(empty, empty);
	}

	@Override
	public void scanIncremental(String startDate, String endDate) {
		_scan(Optional.of(startDate), Optional.of(endDate));
	}

	private void _update(boolean incremental) {
		if (dryRun)
			System.out.println("Performing dry run.");
		executor = initThreadPool();
		ArrayList<Interaction> keys = Collections.list(((ConcurrentHashMap<Interaction, UpdateData>) data).keys());
		int totalThreads = 64;

		Map<String, String> keyTypeMap = configuration
				.getTableConfigurations()
				.stream()
				.collect(Collectors.toMap(TableConfiguration::getKey, TableConfiguration::getTypename));

		// add affinity to keyTypeMap
		keyTypeMap.put(configuration.getAffinityKey(), configuration.getAffinityField());

		TicToc.tic();
		for (int thread = 0; thread < totalThreads; thread++) {
			UpdateTask task = new UpdateTask(keys, data, client, counterVector, thread, totalThreads, configuration,
					keyTypeMap, dryRun, incremental);
			executor.execute(task);
		}
		TicToc.toc("for in update with " + totalThreads + " threads.");
		TicToc.tic();
		shutdown();
		TicToc.toc("shutdown");
		System.out.println("Total updates = " + counterVector.stream().mapToInt(i -> i).sum());
	}

	@Override
	public void update() {
		_update(false);
	}

	@Override
	public void updateIncremental() {
		_update(true);
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
			executor = null;
		}
	}

	private static ExecutorService initThreadPool() {
		return Executors.newCachedThreadPool();
	}

	private static Map<Interaction, UpdateData> accumulate(Map<Interaction, UpdateData> previous,
			Vector<Interaction> current, String type) {
		if (current == null)
			return previous;
		// forEach could be changed with a reduce
		current.stream().forEach(interaction -> previous.merge(interaction, new UpdateData(type), UpdateData::merge));
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

	public static Map<Interaction, UpdateData> combineMaps(Map<Interaction, UpdateData> prev,
			Map<Interaction, UpdateData> curr) {
		if (prev.equals(curr))
			return prev;
		curr.forEach((key, value) -> prev.merge(key, value, UpdateData::merge));
		return prev;
	}

}