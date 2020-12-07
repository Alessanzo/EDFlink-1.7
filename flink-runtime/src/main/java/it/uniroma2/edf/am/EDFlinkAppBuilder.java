package it.uniroma2.edf.am;

import it.uniroma2.dspsim.dsp.Application;
import it.uniroma2.dspsim.dsp.ApplicationBuilder;
import it.uniroma2.dspsim.dsp.Operator;
import it.uniroma2.dspsim.dsp.queueing.MG1OperatorQueueModel;
import it.uniroma2.edf.EDFLogger;
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

	static public Application buildApplication(JobGraph jobGraph, ApplicationMonitor appMonitor){
		Application app = new Application();
		double muScalingFactor = 1.0;
		HashMap<String, Operator> names2operators = new HashMap<>();

		final double mu = 180.0 * muScalingFactor;
		final double serviceTimeMean = 1/mu;
		final double serviceTimeVariance = 1.0/mu*1.0/mu/2.0;
		//final int maxParallelism = jobGraph.getMaximumParallelism();
		//creating Application Operators and adding them to it, with same Name as JobVertexes
		for (JobVertex operator: JobGraphUtils.listSortedTopologicallyOperators(jobGraph, true, true)){
			Operator appOp = new EDFlinkOperator(appMonitor, operator, operator.getName(),
				new MG1OperatorQueueModel(serviceTimeMean, serviceTimeVariance), 5);
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
