package lib.main;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import main.Main;

public class MainTest {
	StringBuilder yamlFile = new StringBuilder();
	StringBuilder dryRun = new StringBuilder();
	StringBuilder startDate = new StringBuilder();
	StringBuilder endDate = new StringBuilder();
	String[] args;

	@Test
	public void dryRunChange() {
		args = new String[] {};
		Main.argsCheck(yamlFile, dryRun, startDate, endDate, args);
		// should remain the same because args is empty
		assertEquals(dryRun.toString(), "false");
		args = new String[] { "--dry-run" };
		dryRun.setLength(0);
		Main.argsCheck(yamlFile, dryRun, startDate, endDate, args);
		assertEquals(dryRun.toString(), "true");
	}

	@Test
	public void invalidDates() {
		// start date
		args = new String[] { "--start-date=2020-21-01" };
		Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
			Main.argsCheck(yamlFile, dryRun, startDate, endDate, args);
		});
		assertEquals("Invalid start date", exception.getMessage());

		// end date
		args = new String[] { "--end-date=2020-21-01" };
		exception = assertThrows(IllegalArgumentException.class, () -> {
			Main.argsCheck(yamlFile, dryRun, startDate, endDate, args);
		});
		assertEquals("Invalid end date", exception.getMessage());
	}

	@Test
	public void invalidArgument() {
		args = new String[] { "--invalid-argument" };
		Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
			Main.argsCheck(yamlFile, dryRun, startDate, endDate, args);
		});
		assertEquals("Unsupported argument --invalid-argument", exception.getMessage());
	}
}
