package dataManager;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import affinity.InteracionType;
import affinity.Interaction;
import affinity.InteractionData;
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
	ConcurrentHashMap<Interaction, InteractionData> map;

	public ScanSegmentTask(String tableName, int numberOfThreads,
			int thread, DynamoDbClient client, String field,
			ConcurrentHashMap<Interaction, InteractionData> map,
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
		int totalScannedItemCount = 0, cycle = 0;
		try {
			Map<String, AttributeValue> mapStartKey = null;
			do {
				ScanRequest scanRequest = ScanRequest.builder()
						.tableName(tableName)
						// .limit(2000)
						.filterExpression(
								"attribute_exists(toID) AND " + field
										+ " <> toID")
						.exclusiveStartKey(mapStartKey)
						.segment(thread)
						.totalSegments(numberOfThreads).build();

				ScanResponse scanResponse = client.scan(scanRequest);
				List<Map<String, AttributeValue>> items = scanResponse
						.items();
				totalScannedItemCount += items.size();
				AttributeValue from, to;
				for (Map<String, AttributeValue> item : items) {
					from = item.get(field);
					to = item.get("toID");
					Interaction interaction = new Interaction(from,
							to);
					add(interaction);
				}
				mapStartKey = scanResponse.hasLastEvaluatedKey()
						? scanResponse.lastEvaluatedKey()
						: null;
				cycle++;
			} while (mapStartKey != null);
		} catch (Exception e) {
			System.err.println(e.getMessage());
		} finally {
			System.out.println("Scanned " + totalScannedItemCount
					+ " items in " + cycle + " cycles from segment "
					+ thread + "/" + numberOfThreads + " of "
					+ tableName);
		}
	}

	private void add(Interaction interaction) {
		map.compute(interaction, (key, value) -> {
			if (value == null) {
				value = new InteractionData();
			}
			value.increment(type);
			return value;
		});
	}
}
