package it.uniroma2.edf.am;

import it.uniroma2.dspsim.Configuration;
import it.uniroma2.dspsim.ConfigurationKeys;
import it.uniroma2.dspsim.dsp.Application;
import it.uniroma2.dspsim.dsp.Operator;
import it.uniroma2.dspsim.dsp.edf.EDF;
import it.uniroma2.dspsim.dsp.edf.am.ApplicationManager;
import it.uniroma2.dspsim.dsp.edf.am.ApplicationManagerFactory;
import it.uniroma2.dspsim.dsp.edf.am.ApplicationManagerType;
import it.uniroma2.dspsim.dsp.queueing.MG1OperatorQueueModel;
import it.uniroma2.dspsim.infrastructure.ComputingInfrastructure;
import it.uniroma2.edf.JobGraphUtils;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;

import java.io.*;
import java.util.HashMap;
import java.util.Set;

public class EDFlink extends EDF {

	private Application application;

	public EDFlink(Application application, double sloLatency) {
		//TODO parametro da passare al costruttore JobGraph, da convertire in Application e passarla a super
		super(application, sloLatency);
		this.application = application;
		//LoggingUtils.configureLogging();(?)
		//simulation.dumpConfigs();
		//simulation.dumpStats();
	}
	//TODO TOGLIERE
	public static void initialize(JobGraph jobGraph){
		//TODO spostare all'avvio di flink per leggere la configurazione una volta
		Configuration conf = Configuration.getInstance();
		conf.parseDefaultConfigurationFile();

		//LoggingUtils.configureLogging();(?)
		//TODO spostare all'avvio di flink per leggere il numero di nodi una volta per tutte
		ComputingInfrastructure.initCustomInfrastructure(
			new double[]{1.0, 0.7, 1.3, 0.9, 1.7, 0.8, 1.8, 2.0, 1.65, 1.5},
			conf.getInteger(ConfigurationKeys.NODE_TYPES_NUMBER_KEY, 3));



	}

	public static Application jobGraph2App(JobGraph jobGraph){
		Application app = new Application();
		double muScalingFactor = 1.0;
		HashMap<String, Operator> names2operators = new HashMap<>();

		final double mu = 180.0 * muScalingFactor;
		final double serviceTimeMean = 1/mu;
		final double serviceTimeVariance = 1.0/mu*1.0/mu/2.0;
		final int maxParallelism = jobGraph.getMaximumParallelism();
		//TODO SENZA SORGENTI?
		for (JobVertex operator: jobGraph.getVerticesSortedTopologicallyFromSources()){
			Operator appOp = new Operator(operator.getName(),
				new MG1OperatorQueueModel(serviceTimeMean, serviceTimeVariance), maxParallelism);
			app.addOperator(appOp);
			names2operators.put(operator.getName(), appOp);

			Set<JobVertex> upstreamOperators = JobGraphUtils.listUpstreamOperators(jobGraph, operator);
			if (!upstreamOperators.isEmpty()){
				for (JobVertex upstrop: upstreamOperators) {
					Operator upstrAppOp = names2operators.get(upstrop.getName());
					app.addEdge(upstrAppOp, appOp);
				}
			}
		}

		return app;
	}




	private ApplicationManager newApplicationManager(org.apache.flink.configuration.Configuration configuration
		, JobGraph jobGraph, Dispatcher dispatcher, double sloLatency) {
		return new EDFlinkApplicationManager(configuration, jobGraph, dispatcher, application, sloLatency);
	}

}
