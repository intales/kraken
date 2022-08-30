package config;

import java.util.Collections;
import java.util.List;

public class Configuration {
	private List<TableConfiguration> tableConfigurations;
	private String updateTable;
	private String affinityKey;
	private String affinityField;

	public Configuration() {
	}

	private Configuration(Builder builder) {
		this.tableConfigurations = builder.tableConfigurations;
		this.updateTable = builder.updateTable;
		this.affinityKey = builder.affinityKey;
		this.affinityField = builder.affinityField;
	}

	public List<TableConfiguration> getTableConfigurations() {
		return tableConfigurations;
	}

	public String getUpdateTable() {
		return updateTable;
	}

	public String getAffinityKey() {
		return affinityKey;
	}

	public String getAffinityField() {
		return affinityField;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {
		private List<TableConfiguration> tableConfigurations = Collections.emptyList();
		private String updateTable;
		private String affinityKey;
		private String affinityField;

		private Builder() {
		}

		public Builder withTableConfigurations(List<TableConfiguration> tableConfigurations) {
			this.tableConfigurations = tableConfigurations;
			return this;
		}

		public Builder withUpdateTable(String updateTable) {
			this.updateTable = updateTable;
			return this;
		}

		public Builder withAffinityKey(String affinityKey) {
			this.affinityKey = affinityKey;
			return this;
		}

		public Builder withAffinityField(String affinityField) {
			this.affinityField = affinityField;
			return this;
		}

		public Configuration build() {
			return new Configuration(this);
		}
	}

	@Override
	public String toString() {
		return "Configuration [tableConfigurations=" + tableConfigurations + ", updateTable=" + updateTable
				+ ", affinityKey=" + affinityKey + ", affinityField=" + affinityField + "]";
	}

}
