package it.uniroma2.edf.monitor;

import it.uniroma2.edf.monitor.Monitor;
import it.uniroma2.edf.utils.EDFLogger;
import it.uniroma2.edf.utils.JobGraphUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.shaded.netty4.io.netty.handler.logging.LogLevel;

import java.util.List;

/*Class that extends Monitor and implements methods for HEDFlinkAM to retrieve end-to-end latency
* */
public class ApplicationMonitor extends Monitor {

	protected List<JobVertex> sources, sinks;
	protected List<List<JobVertex>> paths;

	public ApplicationMonitor(JobGraph jobGraph, Configuration configuration) {
		super(jobGraph, configuration);

		this.sources = JobGraphUtils.listSources(jobGraph);
		this.sinks = JobGraphUtils.listSinks(jobGraph);
		this.paths = JobGraphUtils.listSourceSinkPaths(jobGraph);

		EDFLogger.log("Sources:"+ sources, LogLevel.INFO, ApplicationMonitor.class);
		EDFLogger.log("Sinks:"+ sinks, LogLevel.INFO, ApplicationMonitor.class);
		EDFLogger.log("Paths:"+ paths, LogLevel.INFO, ApplicationMonitor.class);
	}

	//calculate processing latency exluding sources and sinks, summing all operators queue times (flik latencies) and
	//processing times
	public double endToEndLatency() {
		double latency = 0.0;
		for(List<JobVertex> path: paths){
			double pathlatency = 0.0;
			for (JobVertex vertex: path){
				if ((!vertex.isInputVertex()) && (!vertex.isOutputVertex())) {
					double operatorlatency = getAvgOperatorLatency(vertex) + getAvgOperatorProcessingTime(vertex);
					pathlatency += operatorlatency;
				}
			}
			latency = Math.max(latency, pathlatency);
		}
		return  latency;
	}

	public double endToEndLatencySourcesSinks() {
		double latency = 0.0;
		for(List<JobVertex> path: paths){
			double pathlatency = 0.0;
			for (JobVertex vertex: path){
				double operatorlatency = getAvgOperatorLatency(vertex) + getAvgOperatorProcessingTime(vertex);
				pathlatency += operatorlatency;
			}
			latency = Math.max(latency, pathlatency);
		}
		return  latency;
	}
}
