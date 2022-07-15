package dynamodb;

import java.util.HashMap;
import java.util.Map;

public class UpdateData {
	Map<String, Integer> attributesMap;

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

	public UpdateData merge(UpdateData other) {
		for (String key : other.attributesMap.keySet()) {
			Integer value = other.attributesMap.get(key);
			add(key, value);
		}
		return this;
	}

	@Override
	public String toString() {
		return "UpdateData [attributesMap=" + attributesMap + "]\n";
	}
}
