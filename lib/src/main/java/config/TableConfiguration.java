package config;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TableConfiguration {
	private String name;
	private String field;
	private String typename;
	private String key;
	private String[] affinityOperations;
	private int threads;
	private double exponent;
	private double weight;

	public TableConfiguration() {
	}

	private TableConfiguration(Builder builder) {
		this.name = builder.name;
		this.field = builder.field;
		this.threads = builder.threads;
		this.typename = builder.typename;
		this.key = builder.key;
		this.affinityOperations = builder.affinityOperations;
		this.weight = builder.weight;
		this.exponent = builder.exponent;
	}

	public double getWeight() {
		return weight;
	}

	public double getExponent() {
		return exponent;
	}

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

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {
		private String name;
		private String field;
		private int threads;
		private String typename;
		private String key;
		private String[] affinityOperations;
		private double weight;
		private double exponent;

		private Builder() {
		}

		public Builder withName(String name) {
			this.name = name;
			return this;
		}

		public Builder withField(String field) {
			this.field = field;
			return this;
		}

		public Builder withThreads(int threads) {
			this.threads = threads;
			return this;
		}

		public Builder withTypename(String typename) {
			this.typename = typename;
			return this;
		}

		public Builder withKey(String key) {
			this.key = key;
			return this;
		}

		public Builder withAffinityOperations(String[] affinityOperations) {
			this.affinityOperations = affinityOperations;
			return this;
		}

		public Builder withWeight(double weight) {
			this.weight = weight;
			return this;
		}

		public Builder withExponent(double exponent) {
			this.exponent = exponent;
			return this;
		}

		public TableConfiguration build() {
			return new TableConfiguration(this);
		}
	}

	@Override
	public String toString() {
		return "TableConfiguration [name=" + name + ", field=" + field + ", typename=" + typename + ", key=" + key
				+ ", affinityOperations=" + Arrays.toString(affinityOperations) + ", threads=" + threads + ", exponent="
				+ exponent + ", weight=" + weight + "]";
	}
}
