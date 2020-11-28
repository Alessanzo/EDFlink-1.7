package it.uniroma2.edf.am;

import it.uniroma2.dspsim.Configuration;
import it.uniroma2.dspsim.ConfigurationKeys;
import it.uniroma2.dspsim.dsp.Application;
import it.uniroma2.dspsim.dsp.Operator;
import it.uniroma2.dspsim.dsp.edf.EDF;
import it.uniroma2.dspsim.dsp.edf.am.ApplicationManager;
import it.uniroma2.dspsim.dsp.edf.am.ApplicationManagerFactory;
import it.uniroma2.dspsim.dsp.edf.am.ApplicationManagerType;
import it.uniroma2.dspsim.dsp.edf.om.OperatorManager;
import it.uniroma2.dspsim.dsp.queueing.MG1OperatorQueueModel;
import it.uniroma2.dspsim.infrastructure.ComputingInfrastructure;
import it.uniroma2.edf.EDFLogger;
import it.uniroma2.edf.JobGraphUtils;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.shaded.netty4.io.netty.handler.logging.LogLevel;

import java.io.*;
import java.util.*;

public class EDFlink extends EDF {

	private Application application;
	private Map<Operator, OperatorManager> operatorManagers;

	public EDFlink(Application application, double sloLatency) {
		super(application, sloLatency);
		this.application = application;

		final List<Operator> operators = application.getOperators();
		Configuration conf = Configuration.getInstance();

		final int numOperators = operators.size();

		operatorManagers = new HashMap<>(numOperators);
		for (Operator op : operators) {
			operatorManagers.put(op, newOperatorManager(op, conf));
		}

		//LoggingUtils.configureLogging();
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

	//PER ORA E' SENZA SORGENTI
	public static Application jobGraph2App(JobGraph jobGraph){
		Application app = new Application();
		double muScalingFactor = 1.0;
		HashMap<String, Operator> names2operators = new HashMap<>();

		final double mu = 180.0 * muScalingFactor;
		final double serviceTimeMean = 1/mu;
		final double serviceTimeVariance = 1.0/mu*1.0/mu/2.0;
		final int maxParallelism = jobGraph.getMaximumParallelism();

		for (JobVertex operator: JobGraphUtils.listSortedTopologicallyOperators(jobGraph, true, true)){
			Operator appOp = new Operator(operator.getName(),
				new MG1OperatorQueueModel(serviceTimeMean, serviceTimeVariance), maxParallelism);
			app.addOperator(appOp);
			names2operators.put(operator.getName(), appOp);

			Set<JobVertex> upstreamOperators = JobGraphUtils.listUpstreamOperators(jobGraph, operator);
			if (!upstreamOperators.isEmpty()){
				for (JobVertex upstrop: upstreamOperators) {
					if (!upstrop.isInputVertex()) {
						Operator upstrAppOp = names2operators.get(upstrop.getName());
						app.addEdge(upstrAppOp, appOp);
					}
				}
			}
		}
		Collection<ArrayList<Operator>> paths = app.getAllPaths();
		for (ArrayList<Operator> path: paths){
			EDFLogger.log("Path Start: ", LogLevel.INFO, EDFlink.class);
			for (Operator pathoperator: path){
				EDFLogger.log("Operator in the path: "+pathoperator.getName(), LogLevel.INFO, EDFlink.class);
			}
		}
		return app;
	}




	public ApplicationManager newApplicationManager(org.apache.flink.configuration.Configuration configuration
		, JobGraph jobGraph, Dispatcher dispatcher, double sloLatency) {
		return new EDFlinkApplicationManager(configuration, jobGraph, dispatcher, application, operatorManagers, sloLatency);
	}

}
