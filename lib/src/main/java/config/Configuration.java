package config;

import java.util.List;

public class Configuration {
	private List<TableConfiguration> tableConfigurations;
	private int updateThreads;
	private String updateTable;
	private String affinityKey;
	private String affinityField;

	public List<TableConfiguration> getTableConfigurations() {
		return tableConfigurations;
	}

	public int getUpdateThreads() {
		return updateThreads;
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

	@Override
	public String toString() {
		return "Configuration [tableConfigurations=" + tableConfigurations + ", updateThreads=" + updateThreads
				+ ", updateTable=" + updateTable + "]";
	}

}
