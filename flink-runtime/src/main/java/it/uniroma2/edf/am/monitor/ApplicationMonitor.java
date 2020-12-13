package it.uniroma2.edf.am.monitor;

import it.uniroma2.dspsim.ConfigurationKeys;
import it.uniroma2.dspsim.dsp.Operator;
import it.uniroma2.dspsim.dsp.edf.om.OMMonitoringInfo;
import it.uniroma2.edf.EDFLogger;
import it.uniroma2.edf.EDFUtils;
import it.uniroma2.edf.JobGraphUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.EDFOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.shaded.netty4.io.netty.handler.logging.LogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

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
	private JedisPool jedisPool = null;

	// must be provided as metrics.reporter.<reporter name>.redishost: ....
	static private final String CONF_REDIS_HOST = "redishost";
	static private final String CONF_REDIS_PORT = "redisport";
	static private final String CONF_LOG_EVERYTHING = "logeverything";





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
		String redisHostname = configuration.getString(CONF_REDIS_HOST, "");
		int redisPort = configuration.getInteger(CONF_REDIS_PORT, 6379);
		jedis = new Jedis(redisHostname, redisPort);

		JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setMaxTotal(128);
		jedisPool = new JedisPool(
			poolConfig,
			redisHostname,
			redisPort);

		//jedis = new Jedis("ec2-3-128-94-177.us-east-2.compute.amazonaws.com", 6379);
		publishOnRedis = true;
		/*
		String redisHostname = configuration.getString(CONF_REDIS_HOST, "");
		publishOnRedis = !redisHostname.isEmpty();
		if (publishOnRedis) {
			int redisPort = configuration.getInteger(CONF_REDIS_PORT, 6379);
			jedis = new Jedis(redisHostname, 6379);
		}
*/
		logEverything = configuration.getBoolean(CONF_LOG_EVERYTHING, false);

		detailedLatencyLogging = configuration.getBoolean(EDFOptions.EDF_AM_DETAILED_LATENCY_LOGGING);
	}

	public void close()
	{
		jedis.close();
	}

	public Jedis getPoolConnection() {
		return jedisPool.getResource();
	}

	public OMMonitoringInfo getOperatorIRandUsage(String operatorName, int currentParallelism) {
		double operatorInputRate = 0;
		double operatorCpuUsage = 0;

		ArrayList<Double> subtaskInputRates = getSubtaskInputRates(operatorName, currentParallelism);
		ArrayList<Double> subtaskCpuUsages = getSubtaskCpuUsages(operatorName, currentParallelism);
		ArrayList<Double> subtaskIRFractions = new ArrayList<>();

		for (double subtaskIR: subtaskInputRates)
			operatorInputRate+=subtaskIR;

		String mode = it.uniroma2.dspsim.Configuration.getInstance().getString(
			ConfigurationKeys.OPERATOR_VALUES_COMPUTING_CASE_KEY, "avg");

		if ((operatorInputRate != 0.0) && (mode.equals("avg"))) {
			for (int i=0; i<currentParallelism;i++) {
				subtaskIRFractions.add(subtaskInputRates.get(i) / operatorInputRate);
				operatorCpuUsage += (subtaskIRFractions.get(i) * subtaskCpuUsages.get(i));
			}
		}
		else if (mode.equals("worst")){
			operatorCpuUsage =  Collections.max(subtaskCpuUsages);
		}
		else {
			for (double subtaskCpuUsage: subtaskCpuUsages)
				operatorCpuUsage += subtaskCpuUsage;
			operatorCpuUsage = operatorCpuUsage / currentParallelism;
		}
		OMMonitoringInfo monitoringInfo = new OMMonitoringInfo();
		monitoringInfo.setInputRate(operatorInputRate);
		monitoringInfo.setCpuUtilization(operatorCpuUsage);
		return monitoringInfo;
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



	public double getSubtaskInputRate(String operator, String subtaskId) {
		String key = String.format("inputRate.%s.%s.%s", jobGraph.getName(), operator, subtaskId);
		log.info("key: " + key);
		String res = jedis.get(key);
		if(res == null) return -1;
		return Double.parseDouble(res);
	}

	public ArrayList<Double> getSubtaskInputRates(String operator, int parallelism){
		ArrayList<Double> subtaskInputRates = new ArrayList<>();
		Jedis jedis = jedisPool.getResource();
		String key = String.format("inputRate.%s.%s.*", jobGraph.getName(), operator);
		ScanParams scanParams = new ScanParams().match(key);
		String cur = SCAN_POINTER_START;
		double inputRatesSum = 0.0;
		do {
			ScanResult<String> scanResult = jedis.scan(cur, scanParams);
			// work with result
			for (String singleKey: scanResult.getResult()){
				int subtaskIndex = Integer.parseInt(singleKey.split("\\.")[3]);

				if (subtaskIndex < parallelism) {
					String value = jedis.get(singleKey);
					subtaskInputRates.add(Double.parseDouble(value));
				}
			}
			cur = scanResult.getCursor();
		} while (!cur.equals(SCAN_POINTER_START));
		jedis.close();
		return subtaskInputRates;
	}

	public double getOperatorInputRate(String operator) {
		Jedis jedis = jedisPool.getResource();
		String key = String.format("inputRate.%s.%s.*", jobGraph.getName(), operator);
		ScanParams scanParams = new ScanParams().match(key);
		String cur = SCAN_POINTER_START;
		double inputRatesSum = 0.0;
		do {
			//ScanResult<String> scanResult = jedis.scan(cur, scanParams);
			ScanResult<String> scanResult = jedis.scan(cur, scanParams);
			// work with result
			for (String singleKey: scanResult.getResult()){
				String value = jedis.get(singleKey);
				inputRatesSum += Double.parseDouble(value);
			}
			cur = scanResult.getCursor();
		} while (!cur.equals(SCAN_POINTER_START));
		jedis.close();
		return inputRatesSum;
	}

	public double getOperatorInputRate(String operator, int parallelism) {
		Jedis jedis = jedisPool.getResource();
		String key = String.format("inputRate.%s.%s.*", jobGraph.getName(), operator);
		ScanParams scanParams = new ScanParams().match(key);
		String cur = SCAN_POINTER_START;
		double inputRatesSum = 0.0;
		do {
			ScanResult<String> scanResult = jedis.scan(cur, scanParams);
			// work with result
			for (String singleKey: scanResult.getResult()){
				int subtaskIndex = Integer.parseInt(singleKey.split("\\.")[3]);

				if (subtaskIndex < parallelism) {
					String value = jedis.get(singleKey);
					inputRatesSum += Double.parseDouble(value);
				}
			}
			cur = scanResult.getCursor();
		} while (!cur.equals(SCAN_POINTER_START));
		jedis.close();
		return inputRatesSum;
	}

	public Map<Operator, Double> getOperatorsInputRate(List<Operator> operators){
		HashMap<Operator, Double> perOperatorInputRates = new HashMap<>();
		List<JobVertex> vertices = jobGraph.getVerticesSortedTopologicallyFromSources();
		ArrayList<JobVertex> clonedVerticesList = new ArrayList<>(vertices);
		for (Operator operator: operators){
			for (JobVertex vertex: clonedVerticesList){
				if (operator.getName().equals(vertex.getName())){
					double operatorInputRate = getOperatorInputRate(vertex.getName());
					perOperatorInputRates.put(operator, operatorInputRate);
					clonedVerticesList.remove(vertex);
				}
			}
		}
		return perOperatorInputRates;
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

	public ArrayList<Double> getSubtaskCpuUsages (String operator, int parallelism) {
		ArrayList<Double> subtaskCpuUsages = new ArrayList<>();
		Jedis jedis = jedisPool.getResource();
		String key = String.format("cpuUsage.%s.%s.*", jobGraph.getName(), operator);
		ScanParams scanParams = new ScanParams().match(key);
		String cur = SCAN_POINTER_START;
		do {
			ScanResult<String> scanResult = jedis.scan(cur, scanParams);

			// work with result
			for (String singleKey: scanResult.getResult()){
				int subtaskIndex = Integer.parseInt(singleKey.split("\\.")[3]);
				Double value = Double.parseDouble(jedis.getSet(singleKey, String.valueOf(0)));
				if (!value.isNaN() && (subtaskIndex < parallelism)) {
					subtaskCpuUsages.add(value);
				}
			}
			cur = scanResult.getCursor();
		} while (!cur.equals(SCAN_POINTER_START));

		jedis.close();
		return subtaskCpuUsages;
	}

	public double getOperatorCpuUsage (String operator, int parallelism) {
		Jedis jedis = jedisPool.getResource();
		String key = String.format("cpuUsage.%s.%s.*", jobGraph.getName(), operator);
		ScanParams scanParams = new ScanParams().match(key);
		String cur = SCAN_POINTER_START;
		double cpuUsageSum = 0.0;
		int subTaskCount = 0;
		do {
			ScanResult<String> scanResult = jedis.scan(cur, scanParams);

			// work with result
			for (String singleKey: scanResult.getResult()){
				int subtaskIndex = Integer.parseInt(singleKey.split("\\.")[3]);
				Double value = Double.parseDouble(jedis.getSet(singleKey,String.valueOf(0)));
				if (!value.isNaN() && (subtaskIndex < parallelism)) {
					cpuUsageSum += value;
					subTaskCount++;
				}
			}
			cur = scanResult.getCursor();
		} while (!cur.equals(SCAN_POINTER_START));

		if(subTaskCount == 0) return 0.0;
		jedis.close();
		return cpuUsageSum / subTaskCount;
	}

	public double getOperatorCpuUsage (Operator operator) {
		Jedis jedis = jedisPool.getResource();
		String key = String.format("cpuUsage.%s.%s.*", jobGraph.getName(), operator.getName());
		int parallelism = operator.getInstances().size();
		ScanParams scanParams = new ScanParams().match(key);
		String cur = SCAN_POINTER_START;
		double cpuUsageSum = 0.0;
		int subTaskCount = 0;
		do {
			ScanResult<String> scanResult = jedis.scan(cur, scanParams);

			// work with result
			for (String singleKey: scanResult.getResult()){
				//from RedisReporter: cpuUsage.jobId.operator.subtaskId -> subtaskIndex is field[3]
				int subtaskIndex = Integer.parseInt(singleKey.split("\\.")[3]);
				Double value = Double.parseDouble(jedis.getSet(singleKey,String.valueOf(0)));
				if (!value.isNaN() && (subtaskIndex < parallelism)) {
					cpuUsageSum += value;
					subTaskCount++;
				}
			}
			cur = scanResult.getCursor();
		} while (!cur.equals(SCAN_POINTER_START));

		if(subTaskCount == 0) return 0.0;
		jedis.close();
		return cpuUsageSum / subTaskCount;
	}

	public double getAvgOperatorProcessingTime (String operator)
	{
		Jedis jedis = jedisPool.getResource();
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
		jedis.close();
		return executionTimeSum / subTaskCount;
	}

	public double getAvgOperatorProcessingTime (JobVertex vertex)
	{
		Jedis jedis = jedisPool.getResource();
		String key = String.format("executionTime.%s.%s.*", jobGraph.getName(), vertex.getName());
		int parallelism = vertex.getParallelism();
		ScanParams scanParams = new ScanParams().match(key);
		String cur = SCAN_POINTER_START;
		double executionTimeSum = 0.0;
		int subTaskCount = 0;
		do {
			ScanResult<String> scanResult = jedis.scan(cur, scanParams);

			// work with result
			for (String singleKey: scanResult.getResult()){
				//from RedisReporter: executionTime.jobId.operator.subtaskId -> subtaskIndex is field[3]
				int subtaskIndex = Integer.parseInt(singleKey.split("\\.")[3]);
				Double value = Double.parseDouble(jedis.get(singleKey));
				if (!value.isNaN() && (subtaskIndex < parallelism)) {
					executionTimeSum += value;
					subTaskCount++;
				}
			}
			cur = scanResult.getCursor();
		} while (!cur.equals(SCAN_POINTER_START));

		if(subTaskCount == 0) return 0.0;
		jedis.close();
		return executionTimeSum / subTaskCount;
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
		//Jedis jedis = getPoolConnection(); //ADD
		Jedis jedis = this.jedisPool.getResource();
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
				int metricSubtask = Integer.parseInt(fields[3]);
				//if metric refers to a subtask in the parallelism range (0, parallelism-1)
				if (metricSubtask < numSubtask) {
					Double[] subtaskLatencySum = subtaskLatencies.get(Integer.parseInt(fields[3]));
					//EDFLogger.log("EDF: Latency sum " + subtaskLatencySum, LogLevel.INFO, ApplicationMonitor.class);
					subtaskLatencySum[0]++;
					subtaskLatencySum[1] += Double.parseDouble(value);
				}
			}
			cur = scanResult.getCursor();
		} while (!cur.equals(SCAN_POINTER_START));
		for (Double[] elem : subtaskLatencies.values()) {
			if (elem[0]!=0.0) operatorLatencySum += (elem[1] / elem[0]);
		}
		jedis.close(); //ADD
		return (operatorLatencySum/numSubtask);
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


}
