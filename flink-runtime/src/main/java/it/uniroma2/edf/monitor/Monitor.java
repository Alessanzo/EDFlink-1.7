package it.uniroma2.edf.monitor;

import it.uniroma2.dspsim.ConfigurationKeys;
import it.uniroma2.dspsim.dsp.Operator;
import it.uniroma2.edf.utils.EDFLogger;
import it.uniroma2.edf.utils.JobGraphUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.EDFOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.shaded.netty4.io.netty.handler.logging.LogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.util.*;

import static java.lang.Double.max;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START;
/*Monitoring class that implements all the basic methods to retrieve metrics, used by both HEDFlinkAM and HEDFlinkOM.
 * */
public class Monitor {

	static private final Logger log = LoggerFactory.getLogger(Monitor.class);

	protected JobGraph jobGraph;

	private Jedis jedis = null;
	private JedisPool jedisPool = null;

	// must be provided as metrics.reporter.<reporter name>.redishost: ....
	static private final String CONF_REDIS_HOST = "metrics.reporter.redisreporter.redishost";
	static private final String CONF_REDIS_PORT = "metrics.reporter.redisreporter.redisport";



	public Monitor(JobGraph jobGraph, Configuration configuration)
	{
		this.jobGraph = jobGraph;

		String redisHostname = configuration.getString(CONF_REDIS_HOST, "");
		int redisPort = configuration.getInteger(CONF_REDIS_PORT, 6379);
		jedis = new Jedis(redisHostname, redisPort);
		//one connection per object is enough because every HEDFlink AM and OM has its own (App/Op) monitor

		JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setMaxTotal(128);
		jedisPool = new JedisPool(
			poolConfig,
			redisHostname,
			redisPort);

	}


	public void close()
	{
		jedis.close();
	}

	public Jedis getPoolConnection() {
		return jedisPool.getResource();
	}

	//retrieve Operator input rate and CPU usage aggregating data for each subtask in use
	public Double[] getOperatorIRandUsage(String operatorName, int currentParallelism) {
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
		return new Double[] {operatorInputRate, operatorCpuUsage};
	}


	//get subtasks input rate list of a specific operator
	public ArrayList<Double> getSubtaskInputRates(String operator, int parallelism){
		ArrayList<Double> subtaskInputRates = new ArrayList<>();
		//Jedis jedis = jedisPool.getResource();
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
		//jedis.close();
		return subtaskInputRates;
	}

	public double getOperatorInputRate(String operator) {
		//Jedis jedis = jedisPool.getResource();
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

	//retrieve subtask cpu usage list
	public ArrayList<Double> getSubtaskCpuUsages (String operator, int parallelism) {
		ArrayList<Double> subtaskCpuUsages = new ArrayList<>();
		//Jedis jedis = jedisPool.getResource();
		String key = String.format("cpuUsage.%s.%s.*", jobGraph.getName(), operator);
		ScanParams scanParams = new ScanParams().match(key);
		String cur = SCAN_POINTER_START;
		do {
			ScanResult<String> scanResult = jedis.scan(cur, scanParams);

			// work with result
			for (String singleKey: scanResult.getResult()){
				int subtaskIndex = Integer.parseInt(singleKey.split("\\.")[3]);
				//Double value = Double.parseDouble(jedis.getSet(singleKey, String.valueOf(0)));
				Double value = Double.parseDouble(jedis.get(singleKey));
				if (!value.isNaN() && (subtaskIndex < parallelism)) {
					subtaskCpuUsages.add(value);
				}
			}
			cur = scanResult.getCursor();
		} while (!cur.equals(SCAN_POINTER_START));

		//jedis.close();
		return subtaskCpuUsages;
	}

	//retrieve average operator proc time combining subtask in use
	public double getAvgOperatorProcessingTime (JobVertex vertex) {
		//Jedis jedis = jedisPool.getResource();
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
				if (!value.isNaN() && (value != 0.0) && (subtaskIndex < parallelism)) {
					executionTimeSum += value;
					subTaskCount++;
				}
			}
			cur = scanResult.getCursor();
		} while (!cur.equals(SCAN_POINTER_START));
		//jedis.close();
		if(subTaskCount == 0) return 0.0;
		return executionTimeSum / subTaskCount;
	}


	//calculate Avg flink Latency up to operator which consists of the sum of the mean queue times up to operator
	public double getAvgLatencyUpToOperator (JobVertex operator)
	{

		double operatorLatencySum = 0.0;
		int numSubtask = operator.getParallelism();
		int subtaskThatReceivedAny = 0;
		HashMap<Integer, Double[]> subtaskLatencies = new HashMap<>();
		for (int subtaskId=0; subtaskId< numSubtask; subtaskId++)
			subtaskLatencies.put(subtaskId, new Double[]{0.0, 0.0});

		String key = String.format("latency.%s.%s.*", jobGraph.getName(), operator.getID());
		ScanParams scanParams = new ScanParams().match(key);
		String cur = SCAN_POINTER_START;
		do {
			ScanResult<String> scanResult = jedis.scan(cur, scanParams);

			// work with result
			for (String singleKey : scanResult.getResult()) {
				String[] fields = singleKey.split("\\.");
				//EDFLogger.log("EDF: Latency key: "+ singleKey + "Latency value: " + value + " Latency fields: " + fields[3], LogLevel.INFO, ApplicationMonitor.class);
				int metricSubtask = Integer.parseInt(fields[3]);
				//if metric refers to a subtask in the parallelism range (0, parallelism-1)
				if (metricSubtask < numSubtask) {
					String value = jedis.get(singleKey);
					double latency = Double.parseDouble(value);
					//EDFLogger.log("EDF: Latency key: "+ singleKey + "Latency value: " + value + " Subtask: " + fields[3], LogLevel.INFO, Monitor.class);
					Double[] subtaskLatencySum = subtaskLatencies.get(metricSubtask);
					//include this (sub)source-subtask latency value to subtask latency mean only if it is not 0
					if (latency != 0.0) {
						subtaskLatencySum[0]++;
						subtaskLatencySum[1] += latency;
					}
				}
			}
			cur = scanResult.getCursor();
		} while (!cur.equals(SCAN_POINTER_START));
		for (Double[] elem : subtaskLatencies.values()) {
			//include this subtask latency value to operator latency mean only if it is not 0
			if (elem[0]!=0.0 && elem[1]!=0.0) {
				operatorLatencySum += (elem[1] / elem[0]);
				subtaskThatReceivedAny++;
			}
		}
		//jedis.close();
		if (subtaskThatReceivedAny != 0)
			return (operatorLatencySum/subtaskThatReceivedAny);
		else return 0.0;
	}

	//get flink latency for an operator, which corresponds to its queue time
	public double getAvgOperatorLatency (JobVertex operator) {
		double max_up_to_me = getAvgLatencyUpToOperator(operator);

		/* Find max latency up to upstream operators */
		double max_up_to_upstream = 0.0;
		Set<JobVertex> upstreamOperators = JobGraphUtils.listUpstreamOperators(jobGraph, operator);
		for (JobVertex upstream : upstreamOperators) {
			//EDFLogger.log("EDF: Operator " + operator.getName() + " upstream operator is " + upstream.getName() +
			//	" with latency " + getAvgLatencyUpToOperator(upstream), LogLevel.INFO, Monitor.class);
			max_up_to_upstream = max(max_up_to_upstream, getAvgLatencyUpToOperator(upstream));
		}
		//it happens working with latency means, but is irrelevant
		if (max_up_to_me < max_up_to_upstream) {
			return 0.0;
		}

		return max_up_to_me - max_up_to_upstream;
	}

}
