package dataManager;

public class InteractionData {
	int likes;
	int comments;
	int collaborations;

	public InteractionData() {
		likes = 0;
		comments = 0;
		collaborations = 0;
	}
	public void increment(int likes, int comments,
			int collaborations) {
		this.likes += likes;
		this.comments += comments;
		this.collaborations += collaborations;
	}

}
