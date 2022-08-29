package main;

public interface DataManager {

	public void scan();

	public void scanIncremental(String startDate, String endDate);

	public void update();

	public void updateIncremental();
}
