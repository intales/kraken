package dynamodb;

import java.util.ArrayList;
import java.util.List;

public class ExpressionBuilder {

	private static final String DELIMITER = ", ";
	private List<String> added;
	private List<String> setted;

	public ExpressionBuilder() {
		added = new ArrayList<>();
		setted = new ArrayList<>();
	}

	public ExpressionBuilder add(String str) {
		added.add(str);
		return this;
	}

	public ExpressionBuilder set(String str) {
		setted.add(str);
		return this;
	}

	public String build() {
		String result = new String();
		result += "ADD ";
		result += String.join(DELIMITER, added);
		result += " SET ";
		result += String.join(DELIMITER, setted);
		return result;
	}
}
