package dynamodb;

import java.util.HashMap;
import java.util.Map;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class Interaction {
	AttributeValue fromID;
	AttributeValue toID;

	public Interaction(AttributeValue from, AttributeValue to) {
		fromID = from;
		toID = to;
	}

	public Map<String, AttributeValue> getMap() {
		Map<String, AttributeValue> map = new HashMap<>();
		map.put("fromID", fromID);
		map.put("toID", toID);
		return map;
	}

	@Override
	public String toString() {
		return fromID + " - " + toID;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((fromID == null) ? 0 : fromID.hashCode());
		result = prime * result
				+ ((toID == null) ? 0 : toID.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Interaction other = (Interaction) obj;
		if (fromID == null) {
			if (other.fromID != null)
				return false;
		} else if (!fromID.equals(other.fromID))
			return false;
		if (toID == null) {
			if (other.toID != null)
				return false;
		} else if (!toID.equals(other.toID))
			return false;
		return true;
	}
}