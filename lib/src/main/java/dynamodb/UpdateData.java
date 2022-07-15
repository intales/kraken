package dynamodb;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class UpdateData {
	private Map<String, Number> attributesMap;

	public UpdateData() {
		attributesMap = new HashMap<>();
	}

	public UpdateData(String key) {
		this();
		increment(key);
	}

	public void increment(String key) {
		add(key, 1);
	}

	public void add(String key, int n) {
		Number value = attributesMap.get(key);
		if (value == null)
			value = n;
		else
			value = value.intValue() + 1;
		attributesMap.put(key, value);
	}

	public static UpdateData merge(UpdateData first, UpdateData second) {
		first.attributesMap.forEach((key, value) -> {
			second.attributesMap.merge(key, value, UpdateData::sum);
		});
		return second;
	}

	public Map<? extends String, ? extends AttributeValue> getMap() {
		return attributesMap
				.entrySet()
				.stream()
				.collect(Collectors.toMap(Entry::getKey, e -> AttributeValue.fromN(String.valueOf(e.getValue()))));
	}

	public String getUpdateExpression(Map<String, String> keyTypeMap) {
		String expression = "";
		String result = attributesMap
				.entrySet()
				.stream()
				.map(entry -> keyTypeMap.get(entry.getKey()) + " = " + entry.getKey())
				.reduce(expression, (exp, str) -> exp + str + ", ");
		return removeLastTwoChars(result);
	}

	public static String removeLastTwoChars(String s) {
		return (s == null || s.length() == 0) ? null : (s.substring(0, s.length() - 2));
	}

	public void computeAffinity(Map<String, List<String>> operations, String affinityKey) {
		long affinity = attributesMap
				.entrySet()
				.stream()
				.mapToLong(entry -> applyOperations(entry, operations).longValue())
				.sum();

		attributesMap.put(affinityKey, affinity);
	}

	private static Number applyOperations(Entry<String, Number> entry, Map<String, List<String>> operationsMap) {
		String key = entry.getKey();
		Number value = entry.getValue();
		List<String> ops = operationsMap.get(key);
		if (ops != null) {
			value = ops.stream().reduce(value, UpdateData::applyFuction, UpdateData::sum);
		}
		return value;
	}

	private static Number applyFuction(Number acc, String ops) {
		return switch (ops) {
		case "increment" -> acc.doubleValue() + 1;
		case "logarithm" -> Math.log((double) acc);
		case "zero" -> 0.0;
		default -> acc;
		};
	}

	private static Number sum(Number number1, Number number2) {
		return number1.doubleValue() + number2.doubleValue();
	}
}
