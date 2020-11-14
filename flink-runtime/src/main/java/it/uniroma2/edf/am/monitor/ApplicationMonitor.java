package it.uniroma2.edf.am.monitor;

import it.uniroma2.edf.JobGraphUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.EDFOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.*;

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
	private Map<String, Map<Integer, Double>> operator2subtasksProcessingTime = new HashMap<>();
	private Map<String, Map<Integer, Double>> operator2subtasksLatency = new HashMap<>();

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
		String key = String.format("inputRate.%s.%s.", jobGraph.getName(), operator);
		ScanParams scanParams = new ScanParams().match(key);
		String cur = SCAN_POINTER_START;
		double inputRatesSum = 0.0;
		do {
			ScanResult<String> scanResult = jedis.scan(cur, scanParams);

			// work with result
			for (String value: scanResult.getResult()){
				inputRatesSum += Double.parseDouble(value);
			}
			cur = scanResult.getCursor();
		} while (!cur.equals(SCAN_POINTER_START));
		return inputRatesSum;
	}

	@Deprecated
	public double getApplicationInputRate()
	{
		String key = String.format("inputRate.%s.", jobGraph.getName());
		ScanParams scanParams = new ScanParams().match(key);
		String cur = SCAN_POINTER_START;
		double inputRatesSum = 0.0;
		do {
			ScanResult<String> scanResult = jedis.scan(cur, scanParams);

			// work with result
			for (String value: scanResult.getResult()){
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

	@Deprecated
	public double getAvgOperatorProcessingTime (String operator)
	{
		String key = String.format("executionTime.%s.%s.", jobGraph.getName(), operator);
		ScanParams scanParams = new ScanParams().match(key);
		String cur = SCAN_POINTER_START;
		double executionTimeSum = 0.0;
		int subTaskCount = 0;
		do {
			ScanResult<String> scanResult = jedis.scan(cur, scanParams);

			// work with result
			for (String value: scanResult.getResult()){
				executionTimeSum += Double.parseDouble(value);
				subTaskCount++;
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


	private double getAvgLatencyUpToOperator (JobVertex operator)
	{
		double latency = 0.0;

		/* Find max latency up to operator subtasks */
		Map<Integer, Double> entries = operator2subtasksLatency.get(operator.getName());
		for (Double val : entries.values()) {
			latency += val;
		}
		latency /= entries.size();

		return latency;
	}

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

	public void updateLatencyUpToOperator(String operator, int subtaskId, double value) {
		if (!operator2subtasksLatency.containsKey(operator)) {
			operator2subtasksLatency.put(operator, new HashMap<>());
		}

		operator2subtasksLatency.get(operator).put(subtaskId, value);
		//LOG.info("Reported latency {}->{}: {}ms", srcId, operatorId, value);
	}

	public void updateOperatorThroughput(String operator, int subtaskId, double value) {
		if (!operator2subtasksThroughput.containsKey(operator)) {
			operator2subtasksThroughput.put(operator, new HashMap<>());
		}

		operator2subtasksThroughput.get(operator).put(subtaskId, value);
		//LOG.info("Output rate for {}-{} = {} tps", operatorId, subtaskId, value);
	}

}
