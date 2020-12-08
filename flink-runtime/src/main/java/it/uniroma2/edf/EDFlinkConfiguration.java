package it.uniroma2.edf;

import it.uniroma2.dspsim.Configuration;

import java.io.InputStream;

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
}
