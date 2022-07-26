package config;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class YAML {
	public static Configuration getConfiguration(String yamlFile)
			throws StreamReadException, DatabindException, IOException {
		if (yamlFile == null)
			throw new Error("YAML file is null");
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		mapper.findAndRegisterModules();
		return mapper.readValue(new File(yamlFile), Configuration.class);
	}

	public static Configuration getConfiguration(File file) throws StreamReadException, DatabindException, IOException {
		if (file == null)
			throw new Error("YAML file (File) is null");
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		mapper.findAndRegisterModules();
		return mapper.readValue(file, Configuration.class);
	}
}
