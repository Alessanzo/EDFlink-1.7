package it.uniroma2.edf.am;

import it.uniroma2.edf.utils.EDFLogger;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.RescalingBehaviour;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.shaded.netty4.io.netty.handler.logging.LogLevel;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class GlobalActuator {



	public GlobalActuator(){
	}


	public void rescale(Dispatcher dispatcher, JobGraph jobGraph, Map<String,Integer> requests) {
		if (requests == null || requests.size() == 0)
			return;

		else{

			String scalingJob = "";

			for (JobVertex vertex : jobGraph.getVerticesSortedTopologicallyFromSources()){
				scalingJob += "," + vertex.getID().toString() + "," + vertex.getParallelism();

			}

			String response = scaleOperators(dispatcher,jobGraph,requests)? "0":"1";

			for (JobVertex vertex : jobGraph.getVerticesSortedTopologicallyFromSources()){
				response += "," + vertex.getID().toString() + "," + vertex.getParallelism();
			}

		}

	}



	private boolean scaleOperators(Dispatcher dispatcher, JobGraph jobGraph, Map<String, Integer>
		requests) {

		try {

			long start = System.currentTimeMillis();


			EDFLogger.log("HEDF: Try to rescale Operators " +
				requests.toString() + " at: " + start, LogLevel.INFO,GlobalActuator.class);


			EDFLogger.log(jobGraph.getJobID().toString()+",Start scaling," + start, LogLevel.DEBUG,GlobalActuator.class);

			//setup rescale operation
			CompletableFuture<Acknowledge> rescaleFuture = dispatcher.rescaleSingleOperator(jobGraph.getJobID(), requests, RescalingBehaviour.STRICT, RpcUtils.INF_TIMEOUT);

			//try to apply rescaling
			rescaleFuture.get();
			long stop = System.currentTimeMillis();
			EDFLogger.log("HEDF:  Rescaled operators " + requests.toString() + " in: " + (stop-start) + " ms", LogLevel.INFO,GlobalActuator.class);
			EDFLogger.log(jobGraph.getJobID().toString()+",scaled job," + (stop-start), LogLevel.DEBUG,GlobalActuator.class);
			return true;
		} catch (InterruptedException e) {
				EDFLogger.log("HEDF: Could not rescale operators: " + requests.toString() + " " + e.getMessage(), LogLevel.WARN,GlobalActuator.class);
		} catch (ExecutionException e) {
				EDFLogger.log("HEDF: Could not rescale operators: " + requests.toString() + " " + e.getMessage(), LogLevel.WARN,GlobalActuator.class);
		}
		return false;
	}

}
