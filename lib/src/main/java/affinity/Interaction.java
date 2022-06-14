package affinity;

public class Interaction {
	String fromID;
	String toID;

	public Interaction(String from, String to) {
		fromID = from;
		toID = to;
	}

	@Override
	public String toString() {
		return fromID + " - " + toID;
	}
}