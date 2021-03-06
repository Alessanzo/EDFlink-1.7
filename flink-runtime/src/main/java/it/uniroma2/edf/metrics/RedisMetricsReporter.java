package it.uniroma2.edf.metrics;

import it.uniroma2.edf.utils.EDFLogger;
import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.shaded.netty4.io.netty.handler.logging.LogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;


public class RedisMetricsReporter implements MetricReporter, Scheduled {


	private static Logger LOG = LoggerFactory.getLogger(RedisMetricsReporter.class);

	private final Map<Counter, String> counters = new HashMap<>();
	private final Map<Gauge<?>, String> gauges = new HashMap();
	private final Map<Histogram, String> histograms = new HashMap();
	private final Map<Meter, String> meters = new HashMap();

	private boolean publishOnRedis;
	private boolean logEverything = false;
	private Jedis jedis = null;

	// must be provided as metrics.reporter.<reporter name>.redishost: ....
	static private final String CONF_REDIS_HOST = "redishost";
	static private final String CONF_REDIS_PORT = "redisport";
	static private final String CONF_LOG_EVERYTHING = "logeverything";

	@Override
	public void open(MetricConfig metricConfig) {


		String redisHostname = metricConfig.getString(CONF_REDIS_HOST, "localhost");
		int redisPort = metricConfig.getInteger(CONF_REDIS_PORT, 6379);
		jedis = new Jedis(redisHostname, redisPort);
		publishOnRedis = true;
		if (jedis != null) EDFLogger.log("EDF: Redis Metric Reporter Connected to Redis!", LogLevel.INFO, RedisMetricsReporter.class);


	}

	@Override
	public void close() {
		if (jedis != null)
			jedis.close();
	}

	@Override
	public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
		String name = group.getMetricIdentifier(metricName);
		synchronized (this) {
			if (metric instanceof Counter) {
				counters.put((Counter) metric, name);
			} else if (metric instanceof Gauge<?>) {
				gauges.put((Gauge<?>) metric, name);
			} else if (metric instanceof Meter) {
				meters.put((Meter) metric, name);
			} else if (metric instanceof Histogram) {
				histograms.put((Histogram) metric, name);
			}
		}
	}

	@Override
	public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
		synchronized (this) {
			if (metric instanceof Counter) {
				counters.remove(metric);
			} else if (metric instanceof Gauge<?>) {
				this.gauges.remove(metric);
			} else if (metric instanceof Histogram) {
				this.histograms.remove(metric);
			} else if (metric instanceof Meter) {
				this.meters.remove(metric);
			}
		}
	}

	// This is called every 10 seconds (at least using default configuration)
	@Override
	public void report() {
		/*
			for (Map.Entry<Counter, String> metric : counters.entrySet()) {
				LOG.info("{}: {}", metric.getValue(), metric.getKey().getCount());
			}

*/

		for (Map.Entry<Gauge<?>, String> metric : gauges.entrySet()) {
				//LOG.info("{}: {}", metric.getValue(), metric.getKey().getValue().toString());
				final String identifier = metric.getValue();
				if (identifier.contains(MetricNames.CPU_USAGE)) {
					EDFLogger.log("CPU USAGEEE", LogLevel.INFO, RedisMetricsReporter.class);
					LOG.info("{}: {}", metric.getValue(), metric.getKey().getValue().toString());
					Double cpuUsage = (Double) metric.getKey().getValue();
					reportCpuUsage(identifier, cpuUsage);
				}
		}

		for (Map.Entry<Meter, String> metric : meters.entrySet()) {
			if (logEverything) {
				LOG.info("{}: {}", metric.getValue(), metric.getKey().getRate());
			}

			final String identifier = metric.getValue();
			if (identifier.contains("RecordsInPerSecond")) {
				final double rate = metric.getKey().getRate();
				reportInputRate(identifier, rate);
			} else if (identifier.contains("RecordsOutPerSecond")) {
				final double rate = metric.getKey().getRate();
				reportOutputRate(identifier, rate);
			}
		}

		for (Map.Entry<Histogram, String> metric : histograms.entrySet()) {
			final String identifier = metric.getValue();
			if (identifier.contains(MetricNames.EXECUTION_TIME)) {
				HistogramStatistics stats = metric.getKey().getStatistics();
				reportExecutionTime(identifier, stats);
			} else if (identifier.contains("latency.source_id")) {
				HistogramStatistics stats = metric.getKey().getStatistics();
				reportLatency(identifier, stats);
			}

			if (logEverything) {
				dumpHistogramMetric(metric);
			}
		}
	}

	private void dumpHistogramMetric (Map.Entry<Histogram, String> metric)
	{
		HistogramStatistics stats = metric.getKey().getStatistics();
		LOG.info("{}: count:{} min:{} max:{} mean:{} stddev:{} p50:{} p75:{} p95:{} p98:{} p99:{} p999:{}",
			metric.getValue(), stats.size(), stats.getMin(), stats.getMax(), stats.getMean(), stats.getStdDev(),
			stats.getQuantile(0.50), stats.getQuantile(0.75), stats.getQuantile(0.95),
			stats.getQuantile(0.98), stats.getQuantile(0.99), stats.getQuantile(0.999));
	}


	private void reportInputRate (String metricId, double rate)
	{
		// Format: <hostname>.taskmanager.<taskmanagerid>.<jobid>.<operatorname>.<subtaskid>.<metric>
        String fields[] = metricId.split("\\.");

        final String jobId = fields[3];
		final String operator = fields[4];
        final String subtaskId = fields[5];

        if (publishOnRedis) {
			String key = String.format("inputRate.%s.%s.%s", jobId, operator, subtaskId);
            String status = jedis.set(key, String.valueOf(rate));
            //LOG.info("Publishing status: "+status);
			//LOG.info("J={}, operator={}, subtask={}, input rate = {}", jobId, operator, subtaskId, rate);
		} else {
			LOG.info("J={}, operator={}, subtask={}, input rate = {}", jobId, operator, subtaskId, rate);
		}
	}

	private void reportOutputRate (String metricId, double rate)
	{
		// Format: <hostname>.taskmanager.<taskmanagerid>.<jobid>.<operatorname>.<subtaskid>.<metric>
		String fields[] = metricId.split("\\.");

		final String jobId = fields[3];
		final String operator = fields[4];
		final String subtaskId = fields[5];

		if (publishOnRedis) {
			String key = String.format("outputRate.%s.%s.%s", jobId, operator, subtaskId);
			jedis.set(key, String.valueOf(rate));
		} else {
			LOG.info("J={}, operator={}, subtask={}, output rate = {}", jobId, operator, subtaskId, rate);
		}
	}

	private void reportCpuUsage (String metricId, double cpuUsage) {
		String fields[] = metricId.split("\\.");

		final String jobId = fields[3];
		final String operator = fields[4];
		final String subtaskId = fields[5];

		String key = String.format("cpuUsage.%s.%s.%s", jobId, operator, subtaskId);
		jedis.set(key, String.valueOf(cpuUsage));
	}

	private void reportExecutionTime (String metricId, HistogramStatistics stats)
	{
		// Format: <hostname>.taskmanager.<tmid>.<jobid>.<operatorname>.<subtaskid>.<metric>
		String fields[] = metricId.split("\\.");

		final String jobId = fields[3];
		final String operator = fields[4];
		final String subtaskId = fields[5];

		EDFLogger.log("EDF: execution time " + stats.getMean() + ", operator "+ operator +", subtask "+subtaskId, LogLevel.INFO, RedisMetricsReporter.class);

		if (publishOnRedis) {
			String key = String.format("executionTime.%s.%s.%s", jobId, operator, subtaskId);
			jedis.set(key, String.valueOf(stats.getMean()));
			LOG.info("J={}, operator={}, subtask={}, exec time = {}", jobId, operator, subtaskId, stats.getMean());
		} else {
			LOG.info("J={}, operator={}, subtask={}, input rate = {}", jobId, operator, subtaskId, stats.getMean());
		}
	}

	private void reportLatency (String metricId, HistogramStatistics stats)
	{
		// Format: <hostname>.taskmanager.<tmid>.<jobid>.latency.source_id.<source_id>.
		// source_subtask_index.<src_subtask>.operator_id.<operatorid>.operator_subtask_id.<subtaskid>.<metric>
		String fields[] = metricId.split("\\.");

		final String jobId = fields[3];
		final String sourceId = fields[6];
		String operator;
		String subtaskId;
		String sourceSubtaskId;
		if (fields.length == 12){
			sourceSubtaskId = String.valueOf(0);
			operator = fields[8];
			subtaskId = fields[10];
		}
		else {
			sourceSubtaskId = fields[8];
			operator = fields[10];
			subtaskId = fields[12];
		}

		//EDFLogger.log("HEDF: latency " + stats.getMean() + ", operator "+ operator +", subtask"+subtaskId, LogLevel.INFO, RedisMetricsReporter.class);

		if (publishOnRedis) {
			//String key = String.format("latency.%s.%s.%s.%s.%s", jobId, sourceId, sourceSubtaskId, operator, subtaskId);
			String key = String.format("latency.%s.%s.%s.%s.%s", jobId, operator, subtaskId, sourceId, sourceSubtaskId);
			jedis.set(key, String.valueOf(stats.getMean()));
		} else {
			LOG.info("J={}, operator={}, subtask={}, input rate = {}", jobId, operator, subtaskId, stats.getMean());
		}
	}
}

