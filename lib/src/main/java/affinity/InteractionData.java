package affinity;

import java.util.HashMap;
import java.util.Map;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class InteractionData {
	public int likes;
	public int comments;
	public int collaborations;

	public InteractionData() {
		likes = 0;
		comments = 0;
		collaborations = 0;
	}

	public Map<String, AttributeValue> getMap() {
		Map<String, AttributeValue> map = new HashMap<>();

		map.put(":l", AttributeValue.fromN("" + likes));
		map.put(":cm", AttributeValue.fromN("" + comments));
		map.put(":cl", AttributeValue.fromN("" + collaborations));

		return map;
	}

	@Override
	public String toString() {
		return "\t" + likes + "\t" + comments + "\t" + collaborations;
	}
}
