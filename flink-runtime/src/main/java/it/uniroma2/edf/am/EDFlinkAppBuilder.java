package it.uniroma2.edf.am;

import it.uniroma2.dspsim.Configuration;
import it.uniroma2.dspsim.ConfigurationKeys;
import it.uniroma2.dspsim.dsp.Application;
import it.uniroma2.dspsim.dsp.ApplicationBuilder;
import it.uniroma2.dspsim.dsp.Operator;
import it.uniroma2.dspsim.dsp.queueing.MG1OperatorQueueModel;
import it.uniroma2.edf.EDFLogger;
import it.uniroma2.edf.EDFlinkConfiguration;
import it.uniroma2.edf.JobGraphUtils;
import it.uniroma2.edf.am.monitor.ApplicationMonitor;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.shaded.netty4.io.netty.handler.logging.LogLevel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

public class EDFlinkAppBuilder extends ApplicationBuilder {

	static public Application buildApplication(JobGraph jobGraph){
		Application app = new Application();
		double muScalingFactor = 1.0;
		HashMap<String, Operator> names2operators = new HashMap<>();

		final double mu = 6.0 * muScalingFactor;
		final double serviceTimeMean = 1/mu;
		final double serviceTimeVariance = 1.0/mu*1.0/mu/2.0;

		Configuration conf = EDFlinkConfiguration.getEDFlinkConfInstance();
		//creating Application Operators and adding them to it, with same Name as JobVertexes
		final int maxParallelism = conf.getInteger(ConfigurationKeys.OPERATOR_MAX_PARALLELISM_KEY, 5);
		EDFLogger.log("EDF: Operator Max Parallelism " + maxParallelism, LogLevel.INFO, EDFlinkAppBuilder.class);
		String[] operatorsServiceStats= conf.getString("simulation.service.stats", "").split(",");
		int operatorNum = JobGraphUtils.listOperators(jobGraph, true, true).size();
		double[] serviceStats = new double[2*operatorNum];
		if (operatorsServiceStats.length < (2* operatorNum)){
			for (int i=0;i<2*operatorNum;i = i+2){
				serviceStats[i] = 0.25;
				serviceStats[i+1] = 0.0;
			}
		}
		else {
			for (int i=0;i<2*operatorNum;i++){
				try {
					serviceStats[i] = Double.parseDouble(operatorsServiceStats[i]);
				} catch (NumberFormatException e){
					for (int j=0;j<2*operatorNum;j = j+2){
						serviceStats[j] = 0.25;
						serviceStats[j+1] = 0.0;
					}
				}
			}
		}

		int ops = 0;
		for (JobVertex operator: JobGraphUtils.listSortedTopologicallyOperators(jobGraph, true, true)){

			Operator appOp = new EDFlinkOperator(operator, operator.getName(),
					new MG1OperatorQueueModel(serviceStats[ops], serviceStats[ops+1]), maxParallelism);
			EDFLogger.log("EDF: Operator "+ operator.getName() +" with service time mean "+ serviceStats[ops]+
				" and variance "+serviceStats[ops+1], LogLevel.INFO, EDFlinkAppBuilder.class);
			ops = ops +2;
			app.addOperator(appOp);
			names2operators.put(operator.getName(), appOp);

			//creating Edges between Operators
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
		computeOperatorsSLO(app);
		return app;
	}

}
