package config;

public class TableConfiguration {
    private String name;
    private String field;
    private int threads;
    private String typename;
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
	@Override
	public String toString() {
		return "TableConfiguration [name=" + name + ", field=" + field + ", threads=" + threads + ", type=" + typename
				+ "]";
	}
}
