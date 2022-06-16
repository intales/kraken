package affinity;

import java.time.Duration;
import java.time.Instant;

import dataManager.DynamoDataManager;

public class Main {

	public Main() {
	}

	public static void main(String[] args) throws Exception {
		DynamoDataManager db = new DynamoDataManager();
		Instant start = Instant.now();
		db.getLikes().getComments().getCollaborations();
		db.shutdown();
		db.updateInteraction();
		db.shutdown();
		Instant finish = Instant.now();
		System.out.println("Time = "
				+ Duration.between(start, finish).toMillis() + " ms");
		db.count();
	}

}
