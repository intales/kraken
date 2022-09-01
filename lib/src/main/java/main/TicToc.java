package main;

import java.time.Duration;
import java.time.Instant;

public class TicToc {
	static Instant start;
	static Instant end;

	public static void tic() {
		start = Instant.now();
	}

	public static void toc(String func) {
		end = Instant.now();
		Duration timeElapsed = Duration.between(start, end);
		System.out.println("Time taken by " + func + ": " + timeElapsed.toMillis() + " milliseconds");
	}
}
