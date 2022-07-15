package dynamodb;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class UpdateData {
	private Map<String, Integer> attributesMap;

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
		Integer value = attributesMap.get(key);
		if (value == null)
			value = n;
		else
			value += n;
		attributesMap.put(key, value);
	}

	public static UpdateData merge(UpdateData first, UpdateData second) {
		first.attributesMap.forEach((key, value) -> {
			second.attributesMap.merge(key, value, Integer::sum);
		});
		return second;
	}

	@Override
	public String toString() {
		return "UpdateData [attributesMap=" + attributesMap + "]\n";
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
		return removeLastChar(result);
	}

	public static String removeLastChar(String s) {
		return (s == null || s.length() == 0) ? null : (s.substring(0, s.length() - 1));
	}
}
