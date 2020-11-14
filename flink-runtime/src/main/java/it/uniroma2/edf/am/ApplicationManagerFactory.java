package it.uniroma2.edf.am;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.EDFOptions;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationManagerFactory {

	static final private Logger log = LoggerFactory.getLogger(ApplicationManagerFactory.class);

	private ApplicationManagerFactory() {}

	static public ApplicationManager newApplicationManager (Configuration conf, JobGraph jobGraph, Dispatcher dispatcher) {
		String amType = conf.getString(EDFOptions.EDF_AM_TYPE, "");
		log.info("Selected AM type: {}", amType);

		if (amType.equals("mead")) {
			log.info("Launching MeadAM");
			//return new MeadAM(jobGraph, conf, dispatcher);
			return new ApplicationManager(jobGraph, conf, dispatcher);
		} else {
			log.info("Launching default AM");
			return new ApplicationManager(jobGraph, conf, dispatcher);
		}
	}
}
