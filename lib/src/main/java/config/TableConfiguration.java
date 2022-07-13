package config;

public class TableConfiguration {
    private String name;
    private String field;
    private int threads;
    private String type;
	public String getName() {
		return name;
	}
	public String getField() {
		return field;
	}
	public int getThreads() {
		return threads;
	}
	public String getType() {
		return type;
	}
	@Override
	public String toString() {
		return "TableConfiguration [name=" + name + ", field=" + field + ", threads=" + threads + ", type=" + type
				+ "]";
	}
}
