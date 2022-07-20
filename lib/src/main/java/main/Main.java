package main;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;

import config.Configuration;
import config.YAML;
import dynamodb.DynamoDB;

public class Main {

	private static final int MISSING_EXIT_CODE = 1;
	private static final int INVALID_EXIT_CODE = 1;

	public static void argsCheck(StringBuilder yamlFile, StringBuilder dryRun, StringBuilder startDate,
			StringBuilder endDate, String[] args) {
		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		sdf.setLenient(false);
		for (int i = 0; i < args.length; i++) {
			String arg = args[i];
			int equalIndex = arg.indexOf("=");
			// check yaml file path
			if (arg.startsWith("--config-file") && equalIndex != -1) {
				yamlFile.setLength(0);
				yamlFile.append(arg.substring(equalIndex + 1));
			}
			if (arg.equals("--dry-run")) {
				dryRun.append("true");
			}
			// 2022-03-27T02:00:00
			if (arg.startsWith("--start-date") && equalIndex != -1) {
				try {
					String date = arg.substring(equalIndex + 1);
					sdf.parse(date);
					startDate.append(date);
				} catch (ParseException e) {
					System.err.println(e.getMessage());
					System.exit(INVALID_EXIT_CODE);
				}
			}
			if (arg.startsWith("--end-date") && equalIndex != -1) {
				try {
					String date = arg.substring(equalIndex + 1);
					sdf.parse(date);
					endDate.append(date);
				} catch (ParseException e) {
					System.err.println(e.getMessage());
					System.exit(INVALID_EXIT_CODE);
				}
			}
		}
		if (startDate.isEmpty()) {
			System.err.println("--start-date is missing.");
			System.exit(MISSING_EXIT_CODE);
		}
		if (endDate.isEmpty()) {
			System.err.println("--end-date is missing.");
			System.exit(MISSING_EXIT_CODE);
		}
	}

	public static void main(String... args) {
		StringBuilder defaultYamlFile = new StringBuilder("config/source.yaml");
		StringBuilder dryRun = new StringBuilder("");
		StringBuilder startDate = new StringBuilder("");
		StringBuilder endDate = new StringBuilder("");

		argsCheck(defaultYamlFile, dryRun, startDate, endDate, args);

		// Step 1: read configuration file
		Configuration configuration = null;
		try {
			configuration = YAML.getConfiguration(defaultYamlFile.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		boolean dryRunBool = Boolean.valueOf(dryRun.toString());
		System.out.println("Start date = " + startDate);
		System.out.println("End date   = " + endDate);
		DynamoDB dynamo = new DynamoDB(configuration, dryRunBool, startDate.toString(), endDate.toString());
		Instant start = Instant.now();

		// Step 2: scan and aggregate data
		dynamo.scan();

		// Step 3: update table
		dynamo.update();
		Instant finish = Instant.now();
		System.out.println("Time = " + Duration.between(start, finish).toMillis() + " ms");
	}

}
