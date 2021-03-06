package it.uniroma2.edf.am;

import it.uniroma2.dspsim.Configuration;
import it.uniroma2.dspsim.ConfigurationKeys;
import it.uniroma2.dspsim.dsp.Application;
import it.uniroma2.dspsim.dsp.ApplicationBuilder;
import it.uniroma2.dspsim.dsp.Operator;
import it.uniroma2.dspsim.dsp.queueing.MG1OperatorQueueModel;
import it.uniroma2.edf.HEDFlink;
import it.uniroma2.edf.utils.EDFLogger;
import it.uniroma2.edf.HEDFlinkConfiguration;
import it.uniroma2.edf.utils.JobGraphUtils;
import it.uniroma2.edf.om.HEDFlinkOperator;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.shaded.netty4.io.netty.handler.logging.LogLevel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

/*Class that creates an Application object based on the real DAG structure of the Job to execute. Application is used by policies
* to calculate scaling decisions*/
public class HEDFlinkAppBuilder extends ApplicationBuilder {

	static public Application buildApplication(JobGraph jobGraph){
		Application app = new Application();
		HashMap<String, Operator> names2operators = new HashMap<>();


		Configuration conf = HEDFlinkConfiguration.getEDFlinkConfInstance();
		//creating Application Operators and adding them to it, with same Name as JobVertexes
		final int maxParallelism = conf.getInteger(ConfigurationKeys.OPERATOR_MAX_PARALLELISM_KEY, 5);
		EDFLogger.log("HEDF: Operator Max Parallelism: " + maxParallelism, LogLevel.INFO, HEDFlinkAppBuilder.class);
		//service time mean and variance for each intermediate operator is taken by config.properties
		String[] operatorsServiceStats= conf.getString("simulation.service.stats", "").split(",");
		int operatorNum = JobGraphUtils.listOperators(jobGraph, true, true).size();
		double[] serviceStats = new double[2*operatorNum];
		if (operatorsServiceStats.length < (2* operatorNum)){ //if number of times passed is wrong, default parameters are used
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

			//creating operators
			Operator appOp = new HEDFlinkOperator(operator, operator.getName(),
					new MG1OperatorQueueModel(serviceStats[ops], serviceStats[ops+1]), maxParallelism);

			EDFLogger.log("HEDF: Operator "+ operator.getName() +" with service time mean: "+ serviceStats[ops]+
				" and variance: "+serviceStats[ops+1], LogLevel.INFO, HEDFlinkAppBuilder.class);

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
			EDFLogger.log("HEDF: Path Start: ", LogLevel.INFO, HEDFlink.class);
			for (Operator pathoperator: path){
				EDFLogger.log("HEDF: Operator in the path: "+pathoperator.getName(), LogLevel.INFO, HEDFlink.class);
			}
		}
		computeOperatorsSLO(app);
		return app;
	}

}
