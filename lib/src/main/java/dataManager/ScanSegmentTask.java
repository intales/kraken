package dataManager;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import affinity.InteracionType;
import affinity.Interaction;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

public class ScanSegmentTask implements Runnable {
	String tableName;
	int numberOfThreads;
	int thread;
	DynamoDbClient client;
	String field;
	InteracionType type;
	ConcurrentHashMap<Interaction, InteracionType> map;

	public ScanSegmentTask(String tableName, int numberOfThreads,
			int thread, DynamoDbClient client, String field,
			ConcurrentHashMap<Interaction, InteracionType> map,
			InteracionType type) {
		this.tableName = tableName;
		this.numberOfThreads = numberOfThreads;
		this.thread = thread;
		this.client = client;
		this.field = field;
		this.map = map;
		this.type = type;
	}

	@Override
	public void run() {
		// System.out.println("Scanning " + tableName + " segment "
		// + thread + " out of " + numberOfThreads
		// + " segments ");
		int totalScannedItemCount = 0;

		try {
			Map<String, AttributeValue> mapStartKey = null;
			do {
				ScanRequest scanRequest = ScanRequest.builder()
						.tableName(tableName)
						.exclusiveStartKey(mapStartKey)
						.segment(thread)
						.totalSegments(numberOfThreads).build();

				ScanResponse scanResponse = client.scan(scanRequest);
				List<Map<String, AttributeValue>> items = scanResponse
						.items();
				totalScannedItemCount += items.size();
				for (Map<String, AttributeValue> item : items) {
					map.put(new Interaction(item.get(field).s(), ""),
							type);
				}
				if (scanResponse.hasLastEvaluatedKey()) {
					mapStartKey = scanResponse.lastEvaluatedKey();
				} else {
					mapStartKey = null;
				}
			} while (mapStartKey != null);

		} catch (Exception e) {
			System.err.println(e.getMessage());
		} finally {
			System.out.println("Scanned " + totalScannedItemCount
					+ " items from segment " + thread + " out of "
					+ numberOfThreads + " of " + tableName);
		}
	}

}
