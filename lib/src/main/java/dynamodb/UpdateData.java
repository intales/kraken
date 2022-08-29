package dynamodb;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import config.Configuration;
import config.TableConfiguration;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class UpdateData {
	private Map<String, Double> attributesMap;

	public UpdateData() {
		attributesMap = new HashMap<>();
	}

	public UpdateData(String key) {
		this();
		increment(key);
	}

	public void increment(String key) {
		add(key, 1.0);
	}

	public void add(String key, Double n) {
		Double value = attributesMap.get(key);
		if (value == null)
			value = n;
		else
			value = value + n;
		attributesMap.put(key, value);
	}

	public static UpdateData merge(UpdateData first, UpdateData second) {
		first.attributesMap.forEach((key, value) -> {
			second.attributesMap.merge(key, value, Double::sum);
		});
		return second;
	}

	public Map<String, AttributeValue> getMap() {
		return attributesMap
				.entrySet()
				.stream()
				.collect(Collectors.toMap(Entry::getKey, e -> AttributeValue.fromN(e.getValue().toString())));
	}

	public String getUpdateExpression(Map<String, String> keyTypeMap, String separator) {
		String expression = "";
		expression = attributesMap
				.entrySet()
				.stream()
				.map(entry -> keyTypeMap.get(entry.getKey()) + separator + entry.getKey())
				.reduce(expression, (exp, str) -> exp + str + ", ");
		return removeLastTwoChars(expression);
	}

	public static String removeLastTwoChars(String s) {
		return (s == null || s.length() == 0) ? null : (s.substring(0, s.length() - 2));
	}

	public Double computeAffinity(Configuration configuration) {
		return attributesMap.entrySet().stream().mapToDouble(entry -> applyOperations(entry, configuration)).sum();
	}

	public static Map<String, AttributeValue> computeAffinity(Map<String, Double> attributesMap,
			Configuration configuration) {
		Double affinity = attributesMap
				.entrySet()
				.stream()
				.mapToDouble(entry -> applyOperations(entry, configuration))
				.sum();

		attributesMap.put(configuration.getAffinityKey(), affinity);
		Map<String, AttributeValue> affinityMap = new HashMap<>();
		affinityMap.put(configuration.getAffinityKey(), AttributeValue.fromN(affinity.toString()));
		return affinityMap;
	}

	private static Double applyOperations(Entry<String, Double> entry, Configuration configuration) {
		String key = entry.getKey();
		Double value = entry.getValue();
		Optional<TableConfiguration> tableConfOptional = configuration
				.getTableConfigurations()
				.stream()
				.filter(table -> table.getKey() == key)
				.findFirst();

		if (tableConfOptional.isPresent()) {
			TableConfiguration tableConf = tableConfOptional.get();
			value = tableConf.getAffinityOperations().stream().reduce(value, UpdateData::applyFuction, Double::sum);
			// power
			value = Math.pow(value, tableConf.getExponent());
			// multiplication
			value = value * tableConf.getWeight();
		}
		return value;
	}

	private static Double applyFuction(Double acc, String ops) {
		switch (ops) {
		case "increment":
			acc += 1.0;
			break;
		case "logarithm":
			acc = Math.log(acc);
			break;
		default:
		}
		return acc;
	}
}
