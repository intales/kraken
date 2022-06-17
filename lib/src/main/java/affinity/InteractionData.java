package affinity;

import java.util.HashMap;
import java.util.Map;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class InteractionData {
	int likes;
	int comments;
	int collaborations;

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

	public void increment(InteracionType type) {
		switch (type) {
			case like : {
				likes++;
				break;
			}
			case comment : {
				comments++;
				break;
			}
			case collaboration : {
				collaborations++;
				break;
			}
			default :
				throw new IllegalArgumentException(
						"Unexpected value: " + type);
		}
	}

	public int getLikes() {
		return likes;
	}

	public int getComments() {
		return comments;
	}

	public int getCollaborations() {
		return collaborations;
	}
}
