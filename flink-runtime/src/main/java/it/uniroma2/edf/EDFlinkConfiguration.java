package it.uniroma2.edf;

import it.uniroma2.dspsim.Configuration;
import org.apache.flink.configuration.ConfigConstants;

import java.io.*;

public class EDFlinkConfiguration extends Configuration {

	public static synchronized Configuration getEDFlinkConfInstance(){
		if (instance == null) {
			instance = new EDFlinkConfiguration();
		}
		return instance;
	}

	@Override
	public void parseDefaultConfigurationFile () {
		final String propFileName = "config.properties";
		InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
		if (inputStream == null) {
			System.err.println("property file '" + propFileName + "' not found in the classpath");
			return;
		}
		parseConfigurationFile(inputStream);
	}

	public void parseConfigurationFromFile() {
		final String configDir = System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR);
		final File confDirFile = new File(configDir);
		final File yamlConfigFile = new File(confDirFile, "config.properties");
		try {
			InputStream inputStream = new FileInputStream(yamlConfigFile);
			parseConfigurationFile(inputStream);
		}
		catch (IOException e) {
			throw new RuntimeException("Error parsing YAML configuration.", e);
		}
	}
}
