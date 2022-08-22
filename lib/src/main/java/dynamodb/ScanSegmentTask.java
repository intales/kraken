package dynamodb;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

public class ScanSegmentTask implements Callable<Vector<Interaction>> {
	public ScanSegmentTask(DynamoDbClient client, int thread, int totalThreads, String tableName, String fromID,
			Optional<String> startDate, Optional<String> endDate) {
		super();
		this.client = client;
		this.thread = thread;
		this.totalThreads = totalThreads;
		this.tableName = tableName;
		this.fromID = fromID;
		this.startDate = startDate;
		this.endDate = endDate;
	}

	private DynamoDbClient client;
	private int thread;
	private int totalThreads;
	private String tableName;
	private String fromID;
	private String toID = "toID";

	private Optional<String> startDate;
	private Optional<String> endDate;

	@Override
	public Vector<Interaction> call() {
		int cycle = 0;
		Vector<Interaction> items = new Vector<>();
		Map<String, AttributeValue> mapStartKey = null;
		String filterExpression = String.format("attribute_exists(toID) AND (%s <> %s)", fromID, toID);
		String projectionExpression = String.format("%s , %s", fromID, toID);
		Map<String, AttributeValue> expressionMap = new HashMap<>();
		if (startDate.isPresent() && endDate.isPresent()) {
			expressionMap.put(":startDate", AttributeValue.fromS(startDate.get()));
			expressionMap.put(":endDate", AttributeValue.fromS(endDate.get()));
			filterExpression += " AND (createdAt between :startDate AND :endDate)";
		}
		do {
			ScanRequest scanRequest = ScanRequest
					.builder()
					.tableName(tableName)
					// .limit(2000)
					.filterExpression(filterExpression)
					.expressionAttributeValues(expressionMap)
					.projectionExpression(projectionExpression)
					.exclusiveStartKey(mapStartKey)
					.segment(thread)
					.totalSegments(totalThreads)
					.build();

			ScanResponse scanResponse = client.scan(scanRequest);
			items.addAll(scanResponse.items().stream().map(el -> toInteraction(el)).collect(Collectors.toList()));

			mapStartKey = scanResponse.hasLastEvaluatedKey() ? scanResponse.lastEvaluatedKey() : null;
			cycle++;
		} while (mapStartKey != null);

		System.out
				.println("Scanned " + items.size() + " items in " + cycle + " cycles from segment " + thread + "/"
						+ totalThreads + " of " + tableName);
		if (cycle > 1)
			System.out.println(tableName + " could increment the number of threads to " + (totalThreads + cycle - 1));
		return items;
	}

	private Interaction toInteraction(Map<String, AttributeValue> el) {
		return new Interaction(el.get(fromID), el.get(toID));
	}

}
