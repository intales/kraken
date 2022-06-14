package affinity;

import dataManager.DynamoDataManager;

public class Main {

	public Main() {
	}

	public static void main(String[] args) throws Exception {
		DynamoDataManager db = new DynamoDataManager();
		db.getLikes().getComments().getCollaborations();
		db.shutdown();
		db.aggregate();
	}

}
