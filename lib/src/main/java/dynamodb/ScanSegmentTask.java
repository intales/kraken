package dynamodb;

import java.util.Map;
import java.util.Vector;
import java.util.concurrent.Callable;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

public class ScanSegmentTask implements Callable<Vector<Interaction>> {
	public ScanSegmentTask(DynamoDbClient client, int thread, int totalThreads, String tableName, String fromID) {
		super();
		this.client = client;
		this.thread = thread;
		this.totalThreads = totalThreads;
		this.tableName = tableName;
		this.fromID = fromID;
	}

	private DynamoDbClient client;
	private int thread;
	private int totalThreads;
	private String tableName;
	private String fromID;
	private String toID = "toID";

	@Override
	public Vector<Interaction> call() {
		int cycle = 0;
		Vector<Interaction> items = new Vector<>();
		Map<String, AttributeValue> mapStartKey = null;
		do {
			ScanRequest scanRequest = ScanRequest
					.builder()
					.tableName(tableName)
					// .limit(2000)
					.filterExpression("attribute_exists(toID) AND " + fromID + " <> " + toID)
					.projectionExpression(fromID + ", " + toID)
					.exclusiveStartKey(mapStartKey)
					.segment(thread)
					.totalSegments(totalThreads)
					.build();

			ScanResponse scanResponse = client.scan(scanRequest);

			items.addAll(scanResponse.items().stream().map(el -> toInteraction(el)).toList());

			mapStartKey = scanResponse.hasLastEvaluatedKey() ? scanResponse.lastEvaluatedKey() : null;
			cycle++;
		} while (mapStartKey != null);

		System.out
				.println("Scanned " + items.size() + " items in " + cycle + " cycles from segment " + thread + "/"
						+ totalThreads + " of " + tableName);
		if (cycle > 1)
			System.out
					.println(tableName + " could increment the number of threads to " + (totalThreads + cycle - 1)
							+ ".");
		return items;
	}

	private Interaction toInteraction(Map<String, AttributeValue> el) {
		return new Interaction(el.get(fromID), el.get(toID));
	}

}
