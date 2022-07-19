package main;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import config.Configuration;
import config.YAML;
import dynamodb.DynamoDB;

public class Main {

	public static void argsCheck(StringBuilder yamlFile, StringBuilder dryRun, String[] args) {
		for (int i = 0; i < args.length; i++) {
			String arg = args[i];
			int equalIndex = arg.indexOf("=");
			// check yaml file path
			if (arg.startsWith("--config-file") && equalIndex != -1) {
				yamlFile.setLength(0);
				yamlFile.append(arg.substring(equalIndex + 1));
			}
			if (arg.equals("--dryRun")) {
				dryRun.append("true");
			}
		}
	}

	public static void main(String... args) {
		StringBuilder defaultYamlFile = new StringBuilder("config/source.yaml");
		StringBuilder dryRun = new StringBuilder("");

		argsCheck(defaultYamlFile, dryRun, args);

		// Step 1: read configuration file
		Configuration configuration = null;
		try {
			configuration = YAML.getConfiguration(defaultYamlFile.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		DynamoDB dynamo = new DynamoDB(configuration);
		Instant start = Instant.now();
		// Step 2: scan and aggregate data
		dynamo.scan();
		// Step 3: update table
		if (Boolean.valueOf(dryRun.toString())) {
			System.out.println("Performing dry run.");
			dynamo.performDryRun();
		}
		dynamo.update();
		Instant finish = Instant.now();
		System.out.println("Time = " + Duration.between(start, finish).toMillis() + " ms");
	}

}
