package dataManager;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import affinity.Interaction;
import affinity.InteractionData;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public class UpdateTask implements Runnable {
	private DynamoDbClient client;
	private Vector<Integer> counterVector;
	private ArrayList<Interaction> data;
	private int thread;
	private int total;
	private Map<Interaction, InteractionData> map;
	private String interactionTableName;

	public UpdateTask(ArrayList<Interaction> list,
			DynamoDbClient dynamo, Vector<Integer> counterVector,
			int thread, int total,
			Map<Interaction, InteractionData> map, String table) {
		data = list;
		client = dynamo;
		interactionTableName = table;

		this.counterVector = counterVector;
		this.thread = thread;
		this.total = total;
		this.map = map;
	}

	@Override
	public void run() {
		int count = 0;
		Map<String, AttributeValue> attributesMap = new HashMap<>();
		for (int i = thread; i < data.size(); i += total, count++) {
			Interaction key = data.get(i);
			InteractionData val = map.get(key);

			Map<String, AttributeValue> keyMap = key.getMap();

			// createdAt
			attributesMap.put(":c", AttributeValue
					.fromS(new Date().toInstant().toString()));
			// updatedAt
			attributesMap.put(":u", AttributeValue
					.fromS(new Date().toInstant().toString()));
			attributesMap.putAll(val.getMap());

			String expression = "SET updatedAt = :u, createdAt = if_not_exists(createdAt, :c), ";
			expression += "likes = :l, comments = :cm, collaborations = :cl";
			UpdateItemRequest updateRequestItem = UpdateItemRequest
					.builder().tableName(interactionTableName)
					.key(keyMap)
					.expressionAttributeValues(attributesMap)
					.updateExpression(expression).build();
			client.updateItem(updateRequestItem);
		}

		// System.out.println("Thread\t" + Thread.currentThread().getId()
		// + "\tstopping after\t" + count + " updates");
		counterVector.add(Integer.valueOf(count));
	}
}
