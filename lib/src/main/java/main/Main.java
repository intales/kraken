package main;

import java.io.IOException;

import config.Configuration;
import config.YAML;
import dynamodb.DynamoDB;

public class Main {

	public static void argsCheck(StringBuilder yamlFile, String[] args) {
		for (int i = 0; i < args.length; i++) {
			String arg = args[i];
			int equalIndex = arg.indexOf("=");
			// check yaml file path
			if (arg.startsWith("--config-file") && equalIndex != -1) {
				yamlFile.setLength(0);
				yamlFile.append(arg.substring(equalIndex+1));
			}
		}
	}
	
	public static void main(String ... args) {
		StringBuilder defaultYamlFile = new StringBuilder("config/source.yaml");
		
		argsCheck(defaultYamlFile, args);
		
		// Step 1: read configuration file
		Configuration configuration = null;
		try {
			configuration = YAML.getConfiguration(defaultYamlFile.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		DataManager dynamo = new DynamoDB(configuration);
		
		// Step 2: scan and aggregate data
		dynamo.scan();
		// Step 3: update table
		dynamo.update();
		
	}

}
