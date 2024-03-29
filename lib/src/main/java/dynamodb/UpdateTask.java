package dynamodb;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.stream.Collectors;

import config.Configuration;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

public class UpdateTask implements Runnable {

	private DynamoDbClient client;
	private Vector<Integer> counterVector;
	private ArrayList<Interaction> keys;
	private int thread;
	private int totalThreads;
	private Map<Interaction, UpdateData> map;
	private Configuration configuration;
	private Map<String, String> keyTypeMap;
	private boolean dryRun;
	private boolean incremental;

	public UpdateTask(ArrayList<Interaction> keys, Map<Interaction, UpdateData> map, DynamoDbClient client,
			Vector<Integer> counterVector, int thread, int totalThreads, Configuration configuration,
			Map<String, String> keyTypeMap, boolean dryRun, boolean incremental) {
		super();
		this.client = client;
		this.counterVector = counterVector;
		this.keys = keys;
		this.thread = thread;
		this.totalThreads = totalThreads;
		this.map = map;
		this.keyTypeMap = keyTypeMap;
		this.configuration = configuration;
		this.dryRun = dryRun;
		this.incremental = incremental;
	}

	@Override
	public void run() {
		Integer count = 0;

		Map<String, String> keyTypeMapReverse = keyTypeMap
				.entrySet()
				.stream()
				.collect(Collectors.toMap(Entry::getValue, Entry::getKey));

		for (int i = thread; i < keys.size(); i += totalThreads, count++) {
			String date = new Date().toInstant().toString();
			Interaction key = keys.get(i);
			UpdateData value = map.get(key);

			Map<String, AttributeValue> keyMap = key.getMap();
			Map<String, AttributeValue> attributesMap = new HashMap<>();

			attributesMap.putAll(value.getMap());

			// createdAt
			attributesMap.put(":c", AttributeValue.fromS(date));
			// updatedAt
			attributesMap.put(":u", AttributeValue.fromS(date));
			// zero
			attributesMap.put(":zero", AttributeValue.fromN("0"));

			// TODO if incremental use ADD, return new value and update affinity
			// if not use SET and calculate affinity immediately
			ExpressionBuilder expressionBuilder = new ExpressionBuilder();
			expressionBuilder.add("follows :zero");
			expressionBuilder.set("updatedAt = :u");
			expressionBuilder.set("createdAt = if_not_exists(createdAt, :c)");

			if (incremental) {
				// so all the values have to be added
				expressionBuilder.add(value.getUpdateExpression(keyTypeMap, " "));
				String expression = expressionBuilder.build();

				System.out.println("Expression incremental = " + expression);
				UpdateItemRequest updateRequestItem = UpdateItemRequest
						.builder()
						.returnValues(ReturnValue.ALL_NEW)
						.tableName(configuration.getUpdateTable())
						.key(keyMap)
						.expressionAttributeValues(attributesMap)
						.updateExpression(expression)
						.build();
				if (!dryRun) {
					UpdateItemResponse itemResponse = client.updateItem(updateRequestItem);

					Map<String, Double> updatedAttributes = itemResponse
							.attributes()
							.entrySet()
							.stream()
							.filter(entry -> keyTypeMapReverse.containsKey(entry.getKey()))
							.filter(entry -> !entry.getKey().equals(configuration.getAffinityField()))
							.collect(Collectors
									.toMap(entry -> keyTypeMapReverse.get(entry.getKey()),
											entry -> Double.valueOf(entry.getValue().n())));

					expression = String
							.format("SET %s = %s", configuration.getAffinityField(), configuration.getAffinityKey());
					Map<String, AttributeValue> affinity = UpdateData.computeAffinity(updatedAttributes, configuration);
					updateRequestItem = UpdateItemRequest
							.builder()
							.tableName(configuration.getUpdateTable())
							.key(keyMap)
							.expressionAttributeValues(affinity)
							.updateExpression(expression)
							.build();
					client.updateItem(updateRequestItem);
				}
			} else {
				expressionBuilder.set(value.getUpdateExpression(keyTypeMap, " = "));
				expressionBuilder.set(configuration.getAffinityField() + " = " + configuration.getAffinityKey());
				String expression = expressionBuilder.build();
				attributesMap
						.put(configuration.getAffinityKey(),
								AttributeValue.fromN(value.computeAffinity(configuration).toString()));
				UpdateItemRequest updateRequestItem = UpdateItemRequest
						.builder()
						.tableName(configuration.getUpdateTable())
						.key(keyMap)
						.expressionAttributeValues(attributesMap)
						.updateExpression(expression)
						.build();
				if (!dryRun)
					try {
						client.updateItem(updateRequestItem);
					} catch (Exception e) {
						System.err
								.println(e.getMessage() + "\nExpression " + expression + "\nAttributes = "
										+ attributesMap);
					}
			}
		}
		counterVector.add(count);
	}

}
