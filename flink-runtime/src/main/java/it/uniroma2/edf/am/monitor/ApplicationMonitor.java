package it.uniroma2.edf.am.monitor;

import it.uniroma2.edf.EDFLogger;
import it.uniroma2.edf.JobGraphUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.EDFOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.shaded.netty4.io.netty.handler.logging.LogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.*;
import java.util.concurrent.atomic.DoubleAccumulator;

import static java.lang.Double.max;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START;

public class ApplicationMonitor {

	static private final Logger log = LoggerFactory.getLogger(ApplicationMonitor.class);

	protected JobGraph jobGraph;
	protected List<JobVertex> sources, sinks;
	private boolean detailedLatencyLogging;

	private boolean publishOnRedis;
	private boolean logEverything = false;
	private Jedis jedis = null;

	// must be provided as metrics.reporter.<reporter name>.redishost: ....
	static private final String CONF_REDIS_HOST = "redishost";
	static private final String CONF_REDIS_PORT = "redisport";
	static private final String CONF_LOG_EVERYTHING = "logeverything";



	/* Statistics */
	private Map<String, Map<Integer, Double>> operator2subtasksThroughput = new HashMap<>();

	public ApplicationMonitor(JobGraph jobGraph, Configuration configuration)
	{
		this.jobGraph = jobGraph;
		this.sources = JobGraphUtils.listSources(jobGraph);
		this.sinks = JobGraphUtils.listSinks(jobGraph);

		log.info("Sources: {}", sources);
		log.info("Sinks: {}", sinks);
		log.info("Paths: {}", JobGraphUtils.listSourceSinkPaths(jobGraph));

		for (JobVertex jv : jobGraph.getVertices()) {
			log.info("id2name: {} -> {}", jv.getID().toString(), jv.getName());
		}
		jedis = new Jedis("redis", 6379);
		//jedis = new Jedis("ec2-3-128-94-177.us-east-2.compute.amazonaws.com", 6379);
		publishOnRedis = true;
		/*
		String redisHostname = configuration.getString(CONF_REDIS_HOST, "");
		publishOnRedis = !redisHostname.isEmpty();
		if (publishOnRedis) {
			int redisPort = configuration.getInteger(CONF_REDIS_PORT, 6379);
			jedis = new Jedis(redisHostname, 6379);
		}

		logEverything = configuration.getBoolean(CONF_LOG_EVERYTHING, false);
		*/
		detailedLatencyLogging = configuration.getBoolean(EDFOptions.EDF_AM_DETAILED_LATENCY_LOGGING);
	}

	public void close()
	{
		jedis.close();
	}

	public double getSubtaskInputRate(String operator, String subtaskId) {
		String key = String.format("inputRate.%s.%s.%s", jobGraph.getName(), operator, subtaskId);
		log.info("key: " + key);
		String res = jedis.get(key);
		if(res == null) return -1;
		return Double.parseDouble(res);
	}

	public double getOperatorInputRate(String operator) {
		String key = String.format("inputRate.%s.%s.*", jobGraph.getName(), operator);
		ScanParams scanParams = new ScanParams().match(key);
		String cur = SCAN_POINTER_START;
		double inputRatesSum = 0.0;
		do {
			ScanResult<String> scanResult = jedis.scan(cur, scanParams);

			// work with result
			for (String singleKey: scanResult.getResult()){
				String value = jedis.get(singleKey);
				inputRatesSum += Double.parseDouble(value);
			}
			cur = scanResult.getCursor();
		} while (!cur.equals(SCAN_POINTER_START));
		return inputRatesSum;
	}

	public double getApplicationInputRate()
	{
		String key = String.format("inputRate.%s.*", jobGraph.getName());
		ScanParams scanParams = new ScanParams().match(key);
		String cur = SCAN_POINTER_START;
		double inputRatesSum = 0.0;
		do {
			ScanResult<String> scanResult = jedis.scan(cur, scanParams);

			// work with result
			for (String singleKey: scanResult.getResult()){
				String value = jedis.get(singleKey);
				inputRatesSum += Double.parseDouble(value);
			}
			cur = scanResult.getCursor();
		} while (!cur.equals(SCAN_POINTER_START));
		return inputRatesSum;
	}

	public double getApplicationThroughput()
	{
		double rate = 0.0;

		for (JobVertex sink : sinks) {
			String operatorId = sink.getName();
			if (!operator2subtasksThroughput.containsKey(operatorId))
				return 0.0; /* no measurements available yet */

			for (Map.Entry<Integer, Double> e : operator2subtasksThroughput.get(operatorId).entrySet()) {
				rate += e.getValue();
			}
		}

		return rate;
	}

	public double getAvgOperatorProcessingTime (String operator)
	{
		String key = String.format("executionTime.%s.%s.*", jobGraph.getName(), operator);
		ScanParams scanParams = new ScanParams().match(key);
		String cur = SCAN_POINTER_START;
		double executionTimeSum = 0.0;
		int subTaskCount = 0;
		do {
			ScanResult<String> scanResult = jedis.scan(cur, scanParams);

			// work with result
			for (String singleKey: scanResult.getResult()){
				Double value = Double.parseDouble(jedis.get(singleKey));
				if (!value.isNaN()) {
					executionTimeSum += value;
					subTaskCount++;
				}
			}
			cur = scanResult.getCursor();
		} while (!cur.equals(SCAN_POINTER_START));

		if(subTaskCount == 0) return 0.0;
		return executionTimeSum / subTaskCount;
	}

	public double getAvgOperatorThroughput(JobVertex operator)
	{
		String operatorId = operator.getName();
		if (!operator2subtasksThroughput.containsKey(operatorId))
			return 0.0; /* no measurements available yet */

		double tot = 0.0;
		for (Map.Entry<Integer, Double> e : operator2subtasksThroughput.get(operatorId).entrySet()) {
			Double value = e.getValue();
			if (Double.isNaN(value)) {
				continue;
			}
			tot += e.getValue();
		}

		return tot;
	}

	public double getAvgOperatorQueueLength (JobVertex operator)
	{
		return getAvgOperatorThroughput(operator)*getAvgOperatorLatency(operator);
	}

	public double getAvgApplicationResponseTime ()
	{
		double app_latency = 0.0;

		for (List<JobVertex> path : JobGraphUtils.listSourceSinkPaths(jobGraph)) {
			JobVertex src = path.get(0);
			double rPath = 0.0;

            for (JobVertex op : path) {
            	rPath += getAvgOperatorLatency(op);
			}

            //LOG.info("Latency on path proc = {}, queue = {}", r_proc, r_queue);

            app_latency = max(app_latency, rPath);
		}

		return app_latency;
	}

	public double getAvgLatencyUpToOperator (JobVertex operator)
	{
		double operatorLatencySum = 0.0;
		int numSubtask = operator.getParallelism();
		HashMap<Integer, Double[]> subtaskLatencies = new HashMap<>();
		for (int subtaskId=0; subtaskId<= numSubtask; subtaskId++)
			subtaskLatencies.put(subtaskId, new Double[]{0.0, 0.0});

		String key = String.format("latency.%s.%s.*", jobGraph.getName(), operator.getID());
		ScanParams scanParams = new ScanParams().match(key);
		String cur = SCAN_POINTER_START;
		do {
			ScanResult<String> scanResult = jedis.scan(cur, scanParams);

			// work with result
			for (String singleKey : scanResult.getResult()) {
				String value = jedis.get(singleKey);
				String[] fields = singleKey.split("\\.");
				//EDFLogger.log("EDF: Latency key: "+ singleKey + "Latency value: " + value + " Latency fields: " + fields[3], LogLevel.INFO, ApplicationMonitor.class);
				Double[] subtaskLatencySum = subtaskLatencies.get(Integer.parseInt(fields[3]));
				//EDFLogger.log("EDF: Latency sum " + subtaskLatencySum, LogLevel.INFO, ApplicationMonitor.class);
				subtaskLatencySum[0]++;
				subtaskLatencySum[1]+= Double.parseDouble(value);
			}
			cur = scanResult.getCursor();
		} while (!cur.equals(SCAN_POINTER_START));
		for (Double[] elem : subtaskLatencies.values()) {
			if (elem[0]!=0.0) operatorLatencySum += (elem[1] / elem[0]);
		}
		return (operatorLatencySum/numSubtask);
	}

    /*
	public double getAvgLatencyUpToOperator (JobVertex operator)
	{
		double operatorLatencySum = 0.0;
		int numSubtask = operator.getParallelism();
		if (numSubtask == 0) return operatorLatencySum;
		for (int subtaskId=0; subtaskId<numSubtask; subtaskId++) {
			String key = String.format("latency.%s.*", jobGraph.getName());
			ScanParams scanParams = new ScanParams().match(key);
			String cur = SCAN_POINTER_START;
			double subtaskLatencySum = 0.0;
			int numSubtaskLatencies = 0;
			do {
				ScanResult<String> scanResult = jedis.scan(cur, scanParams);

				// work with result
				for (String singleKey : scanResult.getResult()) {
					//subtaskLatencySum += Double.parseDouble(singleKey);
					//numSubtaskLatencies++;
					String pattern = String.format("%s.%s", operator.getID(), subtaskId);
					//EDFLogger.log("EDF: result è " + singleKey + " e pattern " + pattern, LogLevel.INFO, ApplicationMonitor.class);
					if (singleKey.contains(pattern)){
						String value = jedis.get(singleKey);
						//EDFLogger.log("EDF: " + value, LogLevel.INFO, ApplicationMonitor.class);
						subtaskLatencySum += Double.parseDouble(value);
						numSubtaskLatencies++;
					}
				}
				cur = scanResult.getCursor();
			} while (!cur.equals(SCAN_POINTER_START));
			EDFLogger.log("EDF: found " + numSubtaskLatencies + " latency entries for subtask number " + subtaskId, LogLevel.INFO, ApplicationMonitor.class);
			if(numSubtaskLatencies != 0) operatorLatencySum += (subtaskLatencySum / numSubtaskLatencies);
		}
		EDFLogger.log("EDF: found " + numSubtask + " subtasks for operator " + operator.getName(), LogLevel.INFO, ApplicationMonitor.class);
		return (operatorLatencySum/numSubtask);

	}

     */

	public double getAvgOperatorLatency (JobVertex operator) {
		/* QT(op) = max_subtask(QT(op,subtask)) - max_upstream(latency_up_to(upstream) */
		double max_up_to_me = getAvgLatencyUpToOperator(operator);

		/* Find max latency up to upstream operators */
		double max_up_to_upstream = 0.0;
		Set<JobVertex> upstreamOperators = JobGraphUtils.listUpstreamOperators(jobGraph, operator);
        for (JobVertex upstream : upstreamOperators) {
        	max_up_to_upstream = max(max_up_to_upstream, getAvgLatencyUpToOperator(upstream));
		}

        if (max_up_to_me < max_up_to_upstream) {
        	log.error("latency up to {} is less than to upstreams! {}, {}", operator, max_up_to_me, max_up_to_upstream);
		}

		return max_up_to_me - max_up_to_upstream;
	}



	private void logDetailedLatencyInfo()
	{
		int pathIndex = 0;

		for (List<JobVertex> path : JobGraphUtils.listSourceSinkPaths(jobGraph)) {
			JobVertex src = path.get(0);
			double path_latency = 0.0;

			for (JobVertex op : path) {
				double r_op = getAvgOperatorLatency(op);
				path_latency += r_op;

				log.info("Op-{}-latency: {}", op.getName(), r_op);
			}

			//LOG.info("Latency on path proc = {}, queue = {}", r_proc, r_queue);

			++pathIndex;
			log.info("Path-{}: {}", pathIndex, path_latency);
		}
	}


}
