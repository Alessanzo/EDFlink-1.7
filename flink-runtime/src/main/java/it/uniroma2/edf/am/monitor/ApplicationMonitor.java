package it.uniroma2.edf.am.monitor;

import it.uniroma2.edf.JobGraphUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;

import java.util.List;

public class ApplicationMonitor extends Monitor{


	public ApplicationMonitor(JobGraph jobGraph, Configuration configuration) {
		super(jobGraph, configuration);
	}
	//ESCLUDO SORGENTE E SINK DALLE LATENZE SUL PERCORSO
	public double endToEndLatency() {
		double latency = 0.0;
		for(List<JobVertex> path: JobGraphUtils.listSourceSinkPaths(jobGraph)){
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
		for(List<JobVertex> path: JobGraphUtils.listSourceSinkPaths(jobGraph)){
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
