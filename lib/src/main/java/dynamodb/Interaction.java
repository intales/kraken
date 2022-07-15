package dynamodb;

import java.util.Objects;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class Interaction {
	AttributeValue fromID;
	AttributeValue toID;

	public Interaction(AttributeValue from, AttributeValue to) {
		fromID = from;
		toID = to;
	}

	@Override
	public int hashCode() {
		return Objects.hash(fromID, toID);
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
		return Objects.equals(fromID, other.fromID) && Objects.equals(toID, other.toID);
	}

	@Override
	public String toString() {
		return "Interaction [fromID=" + fromID + ", toID=" + toID + "]";
	}
}