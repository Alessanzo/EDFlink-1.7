package org.apache.flink.configuration;


import static org.apache.flink.configuration.ConfigOptions.key;

public class EDFOptions {

	/**
	 * Redis.
	 */
	public static final ConfigOption<String> EDF_REDIS_HOSTNAME = key("edf.redis.hostname").defaultValue("127.0.0.1");
	public static final ConfigOption<Integer> EDF_REDIS_PORT = key("edf.redis.port").defaultValue(6379);

	/**
	 * RabbitMQ.
	 */
	public static final ConfigOption<String> EDF_RABBITMQ_HOSTNAME = key("edf.rabbitmq.hostname").defaultValue("127.0.0.1");
	public static final ConfigOption<String> EDF_RABBITMQ_KPC_REQUEST_QUEUE = key("edf.rabbitmq.kpc.requestqueue").defaultValue("kpcRequests");
	public static final ConfigOption<String> EDF_RABBITMQ_KPC_RESPONSE_QUEUE = key("edf.rabbitmq.kpc.responsequeue").defaultValue("kpcPredictions");
	public static final ConfigOption<String> EDF_RABBITMQ_APPSTATS_QUEUE = key("edf.rabbitmq.appstatsqueue").defaultValue("appStats");

	/**  Aplication Manager
	 *  Types: default, mead
	 */
	public static final ConfigOption<String> EDF_AM_TYPE = key("edf.am.type").defaultValue("default");
	public static final ConfigOption<Integer> AM_INTERVAL_SECS = key("edf.am.interval").defaultValue(10);
	public static final ConfigOption<Integer> AM_ROUNDS_BEFORE_PLANNING = key("edf.am.roundsbeforeplanning").defaultValue(12);

	public static final ConfigOption<String> EDF_AM_STATS_FILENAME = key("edf.am.stats.filename").defaultValue("");
	public static final ConfigOption<Boolean> EDF_AM_DETAILED_LATENCY_LOGGING = key("edf.am.detailedlatencylogging").defaultValue(false);

	public static final ConfigOption<Integer> MIDDLEWARE_RESPTIME_SLO = key("edf.am.middleware.sloms").defaultValue(100);
	public static final ConfigOption<String> MIDDLEWARE_PREDICTION_METHOD = key("edf.am.middleware.prediction.method").defaultValue("map");
	public static final ConfigOption<Integer> MIDDLEWARE_PERCENTILE = key("edf.am.middleware.prediction.percentile").defaultValue(0);
	public static final ConfigOption<Boolean> MIDDLEWARE_ENABLE_SCALING = key("edf.am.middleware.scaling.enabled").defaultValue(true);
	public static final ConfigOption<Integer> MIDDLEWARE_MAX_SCALEDOWN_LEVELS = key("edf.am.middleware.scaling.down.maxlevels").defaultValue(-1);
	public static final ConfigOption<Boolean> MIDDLEWARE_USE_THRESHOLD = key("edf.am.middleware.scaling.usethreshold").defaultValue(false);
	public static final ConfigOption<Double> MIDDLEWARE_UTILIZATION_UPDATE_COEFF = key("edf.am.middleware.utilization.coeff").defaultValue(1.0);
	public static final ConfigOption<Double> MIDDLEWARE_THRESHOLD_SCALE_UP = key("edf.am.middleware.scaling.threshold.up").defaultValue(0.75);
	public static final ConfigOption<Double> MIDDLEWARE_THRESHOLD_SCALE_DOWN = key("edf.am.middleware.scaling.threshold.down").defaultValue(0.50);

	/** Cgroup */
	public static final ConfigOption<Integer> CGROUP_CPU_PERIOD_US = key("edf.cgroup.periodus").defaultValue(5000);
	public static final ConfigOption<String> CGROUP_CPU_QUOTAS_US = key("edf.cgroup.quotasus").defaultValue("5000");
	public static final ConfigOption<Boolean> CGROUP_UTILS_NEED_SUDO = key("edf.cgroup.needsudo").defaultValue(false);
	public static final ConfigOption<Integer> CGROUP_CPU_DEFAULT_LEVEL = key("edf.cgroup.defaultlevel").defaultValue(0);
	public static final ConfigOption<Boolean> CGROUP_DISABLE_CPU_LIMITS = key("edf.cgroup.ignorelimits").defaultValue(false);


	private EDFOptions() {}
}
