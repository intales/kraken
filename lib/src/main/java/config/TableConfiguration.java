package config;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TableConfiguration {
	private String name;
	private String field;
	private int threads;
	private String typename;
	private String key;
	private String[] affinityOperations;
	private double weight;

	public double getWeight() {
		return weight;
	}

	public double getExponent() {
		return exponent;
	}

	private double exponent;

	public String getName() {
		return name;
	}

	public String getField() {
		return field;
	}

	public int getThreads() {
		return threads;
	}

	public String getTypename() {
		return typename;
	}

	public String getKey() {
		return key;
	}

	public List<String> getAffinityOperations() {
		if (affinityOperations == null)
			return Collections.emptyList();
		return Arrays.asList(affinityOperations);
	}
}
