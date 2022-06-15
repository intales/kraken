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
		int totalScannedItemCount = 0;
		try {
			Map<String, AttributeValue> mapStartKey = null;
			do {
				ScanRequest scanRequest = ScanRequest.builder()
						.tableName(tableName)
						// .limit(1000)
						.exclusiveStartKey(mapStartKey)
						.segment(thread)
						.totalSegments(numberOfThreads).build();

				ScanResponse scanResponse = client.scan(scanRequest);
				List<Map<String, AttributeValue>> items = scanResponse
						.items();
				totalScannedItemCount += items.size();

				for (Map<String, AttributeValue> item : items) {
					if (item.containsKey("toID") && (!item.get(field)
							.s().equals(item.get("toID").s()))) {
						Interaction interaction = new Interaction(
								item.get(field).s(),
								item.get("toID").s());
						add(interaction);
					}
				}
				mapStartKey = scanResponse.hasLastEvaluatedKey()
						? scanResponse.lastEvaluatedKey()
						: null;
			} while (mapStartKey != null);
		} catch (Exception e) {
			System.err.println(e.getMessage());
		} finally {
			System.out.println("Scanned " + totalScannedItemCount
					+ " items from segment " + thread + " out of "
					+ numberOfThreads + " of " + tableName);
		}
	}

	private void add(Interaction interaction) {
		InteractionData val = map.get(interaction);
		if (val == null) {
			val = new InteractionData();
			map.put(interaction, val);
		}
		switch (type) {
			case like : {
				val.likes++;
				break;
			}
			case comment : {
				val.comments++;
				break;
			}
			case collaboration : {
				val.collaborations++;
				break;
			}
			default :
				throw new IllegalArgumentException(
						"Unexpected value: " + type);
		}
	}
}
