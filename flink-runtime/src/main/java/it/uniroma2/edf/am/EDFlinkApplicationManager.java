package it.uniroma2.edf.am;

import it.uniroma2.dspsim.ConfigurationKeys;
import it.uniroma2.dspsim.InputRateFileReader;
import it.uniroma2.dspsim.Simulation;
import it.uniroma2.dspsim.dsp.Application;
import it.uniroma2.dspsim.dsp.Operator;
import it.uniroma2.dspsim.dsp.Reconfiguration;
import it.uniroma2.dspsim.dsp.edf.am.ApplicationManager;
import it.uniroma2.dspsim.dsp.edf.om.OMMonitoringInfo;
import it.uniroma2.dspsim.dsp.edf.om.OperatorManager;
import it.uniroma2.dspsim.dsp.edf.om.request.OMRequest;
import it.uniroma2.dspsim.infrastructure.ComputingInfrastructure;
import it.uniroma2.dspsim.stats.Statistics;
import it.uniroma2.dspsim.stats.metrics.CountMetric;
import it.uniroma2.dspsim.stats.metrics.Metric;
import it.uniroma2.dspsim.stats.metrics.RealValuedMetric;
import it.uniroma2.edf.EDFLogger;
import it.uniroma2.edf.JobGraphUtils;
import it.uniroma2.edf.am.execute.GlobalActuator;
import it.uniroma2.edf.am.monitor.ApplicationMonitor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.EDFOptions;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.shaded.netty4.io.netty.handler.logging.LogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class EDFlinkApplicationManager extends ApplicationManager implements Runnable {

	protected JobGraph jobGraph;
	protected Dispatcher dispatcher;
	protected Configuration config;

	protected ApplicationMonitor appMonitor = null;
	protected GlobalActuator globalActuator;
	protected Map<String, Integer> request = new HashMap<>();

	private int amInterval;
	private int roundsBetweenPlanning;

	//Simulation attr
	private final double LATENCY_SLO;

	private InputRateFileReader inputRateFileReader;
	private Application app;

	private Logger logger = LoggerFactory.getLogger(Simulation.class);

	private double wSLO;
	private double wReconf;
	private double wRes;

	private boolean detailedScalingLog;

	/* Statistics */
	private Metric metricViolations;
	private Metric metricReconfigurations;
	private Metric metricResCost;
	private Metric metricAvgCost;
	private Metric[] metricDeployedInstances;

	public EDFlinkApplicationManager(Configuration configuration, JobGraph jobGraph, Dispatcher dispatcher,
									 Application application, double sloLatency) {
		super(application, sloLatency);
		config = configuration;
		this.jobGraph = jobGraph;
		this.dispatcher = dispatcher;
		this.app = application;

		this.amInterval = config.getInteger(EDFOptions.AM_INTERVAL_SECS);
		this.roundsBetweenPlanning = config.getInteger(EDFOptions.AM_ROUNDS_BEFORE_PLANNING);

		this.globalActuator = new GlobalActuator();

		registerMetrics();

		this.LATENCY_SLO = sloLatency;
		it.uniroma2.dspsim.Configuration conf = it.uniroma2.dspsim.Configuration.getInstance();
		this.wSLO = conf.getDouble(ConfigurationKeys.RL_OM_SLO_WEIGHT_KEY, 0.33);
		this.wReconf = conf.getDouble(ConfigurationKeys.RL_OM_RECONFIG_WEIGHT_KEY, 0.33);
		this.wRes = conf.getDouble(ConfigurationKeys.RL_OM_RESOURCES_WEIGHT_KEY, 0.33);

		this.detailedScalingLog = conf.getBoolean(ConfigurationKeys.SIMULATION_DETAILED_SCALING_LOG, false);

		logger.info("SLO latency: {}", LATENCY_SLO);

	}

	@Override
	protected Map<Operator, Reconfiguration> plan(Map<OperatorManager, OMRequest> map, Map<Operator, OMMonitoringInfo> map1) {
		return null;
	}

	protected void initialize()
	{
		EDFLogger.log("EDFlinkAM: INITIALIZED!", LogLevel.INFO, EDFlinkApplicationManager.class);
		this.appMonitor = new ApplicationMonitor(jobGraph, config);
	}

	@Override
	public void run() {

		initialize();
		int round = 0;

		while (true) {
			try {
				//Thread.sleep(amInterval*1000);
				Thread.sleep(3000);
			} catch (InterruptedException e) {
			}

			CompletableFuture<JobStatus> jobStatusFuture = dispatcher.requestJobStatus(jobGraph.getJobID(), Time.seconds(3));
			try {
				EDFLogger.log("AM: GETTING JOB STATUS!", LogLevel.INFO, EDFlinkApplicationManager.class);
				JobStatus jobStatus = jobStatusFuture.get();
				if (jobStatus == JobStatus.FAILED || jobStatus == JobStatus.CANCELED || jobStatus == JobStatus.CANCELLING) {
					EDFLogger.log("AM: CLOSING", LogLevel.INFO, EDFlinkApplicationManager.class);
					close();
					return;
				} else if (jobStatus == JobStatus.FINISHED) {
					EDFLogger.log("AM: JOB FINISHED", LogLevel.INFO, EDFlinkApplicationManager.class);
				} else if (jobStatus != JobStatus.RUNNING) {
					EDFLogger.log("AM: Job Not Running... AM inhibited", LogLevel.INFO, EDFlinkApplicationManager.class);
					continue;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}

			//round = (round + 1) % roundsBetweenPlanning;
			round = (round + 1);
			EDFLogger.log("AM: Round " + round, LogLevel.INFO, EDFlinkApplicationManager.class);
			monitor();

			if ((round % 2) == 0) {
				analyze();
				plan(round);
				execute();
			}
		}
	}

	protected void monitor() {
		EDFLogger.log("Numero di vertici: "+jobGraph.getNumberOfVertices(), LogLevel.INFO, EDFlinkApplicationManager.class);
		EDFLogger.log("Lista di vertici: "+jobGraph.getVertices(), LogLevel.INFO, EDFlinkApplicationManager.class);

		//jobGraph.getVertices().forEach(jobVertex -> LOG.info("operatorids " + jobVertex.getOperatorIDs()));
		//jobGraph.getVertices().forEach(jobVertex -> LOG.info("vertexname " + jobVertex.getName()));
		//jobGraph.getVertices().forEach(jobVertex -> LOG.info("vertexid " + jobVertex.getID()));
		//jobGraph.getVertices().forEach(jobVertex -> LOG.info("vertextostring " + jobVertex.toString()));
		JobGraphUtils.listUpstreamOperators(jobGraph, jobGraph.getVerticesAsArray()[1]).forEach(
			jobVertex -> EDFLogger.log("upstreamtostirng "+jobVertex.toString(), LogLevel.INFO, EDFlinkApplicationManager.class));
		//double ir = appMonitor.getSubtaskInputRate(JobGraphUtils.listOperators(jobGraph, true,true).iterator().next().getOperatorName(),String.valueOf(0));
		if (appMonitor != null) {
			JobVertex vertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(1);
			/*
			double ir = appMonitor.getSubtaskInputRate(jobGraph.getVerticesAsArray()[1].getName(), String.valueOf(0));
			EDFLogger.log("EDF: Input Rate: " + ir, LogLevel.INFO, ApplicationManager.class);
			double operatorIr = appMonitor.getOperatorInputRate(jobGraph.getVerticesAsArray()[1].getName());
			double appIr = appMonitor.getApplicationInputRate();
			double operatorLatency = appMonitor.getAvgLatencyUpToOperator(jobGraph.getVerticesAsArray()[1]);
			double avgOperatorLatency = appMonitor.getAvgOperatorLatency(jobGraph.getVerticesAsArray()[1]);
			double processingTime = appMonitor.getAvgOperatorProcessingTime(jobGraph.getVerticesAsArray()[1].getName());

			 */
			double ir = appMonitor.getSubtaskInputRate(vertex.getName(), String.valueOf(0));
			EDFLogger.log("EDF: Input Rate: " + ir, LogLevel.INFO, it.uniroma2.edf.am.ApplicationManager.class);
			double operatorIr = appMonitor.getOperatorInputRate(vertex.getName());
			double appIr = appMonitor.getApplicationInputRate();
			double operatorLatency = appMonitor.getAvgLatencyUpToOperator(vertex);
			double avgOperatorLatency = appMonitor.getAvgOperatorLatency(vertex);
			double processingTime = appMonitor.getAvgOperatorProcessingTime(vertex.getName());
			EDFLogger.log("EDF: operator Input Rate: " + operatorIr, LogLevel.INFO, it.uniroma2.edf.am.ApplicationManager.class);
			EDFLogger.log("EDF: application Input Rate: " + appIr, LogLevel.INFO, it.uniroma2.edf.am.ApplicationManager.class);
			EDFLogger.log("EDF: operatorLatency: " + operatorLatency, LogLevel.INFO, it.uniroma2.edf.am.ApplicationManager.class);
			EDFLogger.log("EDF: avgOperatorLatency: " + avgOperatorLatency, LogLevel.INFO, it.uniroma2.edf.am.ApplicationManager.class);
			EDFLogger.log("EDF: processingTime: " + processingTime, LogLevel.INFO, it.uniroma2.edf.am.ApplicationManager.class);
			EDFLogger.log("EDF: avgLatency + processingTime: " + (processingTime+avgOperatorLatency), LogLevel.INFO, it.uniroma2.edf.am.ApplicationManager.class);

			//Latencies print for experimentation

		}
	}

	protected void analyze() {
		EDFLogger.log("AM: ANALYZE - parallelism: " +jobGraph.getVerticesAsArray()[1].getParallelism(), LogLevel.INFO, EDFlinkApplicationManager.class);
		//LOG.info("ANALYZE - parallelism: " +jobGraph.getVerticesAsArray()[2].getParallelism());
	}

	protected void plan(int round) {
		if(round == 160){
			/*
			ArrayList<Integer> resTypes = this.jobGraph.getTaskResTypes().get((jobGraph.getVerticesAsArray()[1].getID()));
			resTypes.clear();
			resTypes.add(0);
			this.jobGraph.getTaskResTypes().put(jobGraph.getVerticesAsArray()[1].getID(),resTypes);
			this.request.put(jobGraph.getVerticesAsArray()[1].getID().toString(), 1);
			*/


			for (ArrayList<Integer> list: this.jobGraph.getTaskResTypes().values()){
				list.clear();
				list.add(0);
			}
			this.request.put(jobGraph.getVerticesAsArray()[1].getID().toString(), 1);

			/*
			ArrayList<Integer> resTypes = this.jobGraph.getTaskResTypes().get((jobGraph.getVerticesSortedTopologicallyFromSources().get(1).getID()));
			resTypes.clear();
			resTypes.add(0);
			this.request.put(jobGraph.getVerticesSortedTopologicallyFromSources().get(1).getID().toString(), 1);

			 */
		}
		/*
		else if(round == 8){
			ArrayList<Integer> resTypes = this.jobGraph.getTaskResTypes().get((jobGraph.getVerticesAsArray()[2].getID()));
			resTypes.add(1);
			this.jobGraph.getTaskResTypes().put(jobGraph.getVerticesAsArray()[2].getID(),resTypes);
			this.request.put(jobGraph.getVerticesAsArray()[2].getID().toString(), 3);
		}

		*/

		else{
			//this.request.remove(jobGraph.getVerticesAsArray()[1].getID().toString());
			this.request.clear();
		}

	}

	protected void execute() {
		if (this.request.size() > 0) {
			EDFLogger.log("AM: EXECUTE - RESCALING OPERATORS", LogLevel.INFO, EDFlinkApplicationManager.class);
			globalActuator.rescale(this.dispatcher, this.jobGraph, this.request);
		}
	}

	private void registerMetrics() {
		Statistics statistics = Statistics.getInstance();

		final String STAT_LATENCY_VIOLATIONS = "Violations";
		final String STAT_RECONFIGURATIONS = "Reconfigurations";
		final String STAT_RESOURCES_COST = "ResourcesCost";
		final String STAT_APPLICATION_COST_AVG = "AvgCost";

		this.metricViolations = new CountMetric(STAT_LATENCY_VIOLATIONS);
		statistics.registerMetric(metricViolations);

		this.metricAvgCost = new RealValuedMetric(STAT_APPLICATION_COST_AVG, true, true);
		statistics.registerMetric(metricAvgCost);

		this.metricReconfigurations = new CountMetric(STAT_RECONFIGURATIONS);
		statistics.registerMetric(metricReconfigurations);

		this.metricResCost = new RealValuedMetric(STAT_RESOURCES_COST);
		statistics.registerMetric(metricResCost);

		this.metricDeployedInstances = new RealValuedMetric[ComputingInfrastructure.getInfrastructure().getNodeTypes().length];
		for (int i = 0; i < ComputingInfrastructure.getInfrastructure().getNodeTypes().length; i++) {
			this.metricDeployedInstances[i]	 = new RealValuedMetric("InstancesType" + i);
			statistics.registerMetric(this.metricDeployedInstances[i]);
		}
	}

	protected void close()
	{
		EDFLogger.log("AM: closed", LogLevel.INFO, EDFlinkApplicationManager.class);
		appMonitor.close();
	}
}
