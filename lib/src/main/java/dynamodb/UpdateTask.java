package dynamodb;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public class UpdateTask implements Runnable {

	private DynamoDbClient client;
	private Vector<Integer> counterVector;
	private ArrayList<Interaction> keys;
	private int thread;
	private int totalThreads;
	private Map<Interaction, UpdateData> map;
	private String interactionTableName;
	private Map<String, String> keyTypeMap;
	private Map<String, List<String>> operations;
	private String affinityKey;

	public UpdateTask(ArrayList<Interaction> keys, Map<Interaction, UpdateData> map, DynamoDbClient client,
			Vector<Integer> counterVector, int thread, int totalThreads, String interactionTableName,
			Map<String, String> keyTypeMap, Map<String, List<String>> operations, String affinityKey) {
		super();
		this.client = client;
		this.counterVector = counterVector;
		this.keys = keys;
		this.thread = thread;
		this.totalThreads = totalThreads;
		this.map = map;
		this.interactionTableName = interactionTableName;
		this.keyTypeMap = keyTypeMap;
		this.operations = operations;
		this.affinityKey = affinityKey;
	}

	@Override
	public void run() {
		Integer count = 0;
		Map<String, AttributeValue> attributesMap = new HashMap<>();
		for (int i = thread; i < keys.size(); i += totalThreads, count++) {
			String date = new Date().toInstant().toString();
			Interaction key = keys.get(i);
			UpdateData value = map.get(key);

			Map<String, AttributeValue> keyMap = key.getMap();

			value.computeAffinity(operations, affinityKey);
			attributesMap.putAll(value.getMap());

			// createdAt
			attributesMap.put(":c", AttributeValue.fromS(date));
			// updatedAt
			attributesMap.put(":u", AttributeValue.fromS(date));

			String expression = "SET updatedAt = :u, createdAt = if_not_exists(createdAt, :c), ";

			expression += value.getUpdateExpression(keyTypeMap);

			UpdateItemRequest updateRequestItem = UpdateItemRequest
					.builder()
					.tableName(interactionTableName)
					.key(keyMap)
					.expressionAttributeValues(attributesMap)
					.updateExpression(expression)
					.build();

			client.updateItem(updateRequestItem);

			attributesMap.clear();
		}
		counterVector.add(count);
	}

}
