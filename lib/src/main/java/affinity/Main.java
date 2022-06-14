package affinity;

import java.time.Duration;
import java.time.Instant;

import dataManager.DynamoDataManager;

public class Main {

	public Main() {
	}

	public static void main(String[] args) {
		DynamoDataManager db = new DynamoDataManager();
		Instant start = Instant.now();
		db.getLikes().getComments().getCollaborations();
		db.shutdown();
		Instant end = Instant.now();
		System.out.println(+Duration.between(start, end).toMillis()
				+ " milliseconds");
	}

}
