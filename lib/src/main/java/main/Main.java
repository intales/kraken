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
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;

public class Main {

	private static final int INVALID_EXIT_CODE = 2;
	private static final int MISSING_EXIT_CODE = 1;

	public static void argsCheck(StringBuilder yamlFile, StringBuilder dryRun, StringBuilder startDate,
			StringBuilder endDate, String[] args) throws IllegalArgumentException {
		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		sdf.setLenient(false);
		for (int i = 0; i < args.length; i++) {
			String arg = args[i];
			int equalIndex = arg.indexOf("=");
			// check yaml file path
			if (arg.startsWith("--config-file") && equalIndex != -1) {
				yamlFile.setLength(0);
				yamlFile.append(arg.substring(equalIndex + 1));
			} else if (arg.equals("--dry-run")) {
				dryRun.append("true");
			} else if (arg.startsWith("--start-date") && equalIndex != -1) {
				try {
					String date = arg.substring(equalIndex + 1);
					sdf.parse(date);
					startDate.append(date);
				} catch (ParseException e) {
					System.err.println(e.getMessage());
					throw new IllegalArgumentException("Invalid start date");
				}
			} else if (arg.startsWith("--end-date") && equalIndex != -1) {
				try {
					String date = arg.substring(equalIndex + 1);
					sdf.parse(date);
					endDate.append(date);
				} catch (ParseException e) {
					System.err.println(e.getMessage());
					throw new IllegalArgumentException("Invalid end date");
				}
			} else {
				throw new IllegalArgumentException("Unsupported argument " + arg);
			}
		}
		if (startDate.length() == 0) {
			System.err.println("--start-date\tis missing.");
		}
		if (endDate.length() == 0) {
			System.err.println("--end-date\tis missing.");
		}
		if (dryRun.length() == 0) {
			dryRun.append("false");
		}
	}

	public static void main(String... args) {
		StringBuilder defaultYamlFile = new StringBuilder("config/source.yaml");
		StringBuilder dryRun = new StringBuilder("");
		StringBuilder startDate = new StringBuilder("");
		StringBuilder endDate = new StringBuilder("");

		try {
			argsCheck(defaultYamlFile, dryRun, startDate, endDate, args);
		} catch (IllegalArgumentException e) {
			System.err.println(e.getMessage());
			System.exit(INVALID_EXIT_CODE);
		}

		// Step 1: read configuration file
		Configuration configuration = null;
		try {
			configuration = YAML.getConfiguration(defaultYamlFile.toString());
		} catch (IOException e) {
			System.err.println(e.getMessage());
			System.exit(MISSING_EXIT_CODE);
		}

		boolean dryRunBool = Boolean.valueOf(dryRun.toString());
		DataManager dynamo = new DynamoDB(configuration, ProfileCredentialsProvider.create(), dryRunBool);
		Instant start = Instant.now();

		// Step 2: scan, aggregate and update data
		if (startDate.length() > 0 && endDate.length() > 0) {
			dynamo.scanIncremental(startDate.toString(), endDate.toString());
			dynamo.updateIncremental();
		} else {
			dynamo.scan();
			dynamo.update();
		}
		Instant finish = Instant.now();
		System.out.println("Time = " + Duration.between(start, finish).toMillis() + " ms");
	}

}
