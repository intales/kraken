package dynamodb;

import java.util.HashMap;
import java.util.Map;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class InteractionData {
	Map<String, String> associativeMap;
	Map<String, AttributeValue> attributesMap;
	
	public InteractionData(String key, String ... names) {
		attributesMap = new HashMap<>();
		for (int i = 0; i < names.length; i++) {
			String name = names[i];
			associativeMap.put(name, key + i);
			attributesMap.put(key + 1, AttributeValue.fromN("0"));
		}
	}

}
