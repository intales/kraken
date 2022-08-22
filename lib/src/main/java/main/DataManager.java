package main;

public interface DataManager {

	public void scan();

	public void scanIncremental();

	public void update();

	public void updateIncremental();
}
