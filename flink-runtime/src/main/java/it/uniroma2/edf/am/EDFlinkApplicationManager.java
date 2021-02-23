package it.uniroma2.edf.am;

import it.uniroma2.dspsim.ConfigurationKeys;
import it.uniroma2.dspsim.Simulation;
import it.uniroma2.dspsim.dsp.Application;
import it.uniroma2.dspsim.dsp.Operator;
import it.uniroma2.dspsim.dsp.Reconfiguration;
import it.uniroma2.dspsim.dsp.edf.am.ApplicationManager;
import it.uniroma2.dspsim.dsp.edf.om.OMMonitoringInfo;
import it.uniroma2.dspsim.dsp.edf.om.OperatorManager;
import it.uniroma2.dspsim.dsp.edf.om.request.OMRequest;
import it.uniroma2.dspsim.infrastructure.ComputingInfrastructure;
import it.uniroma2.dspsim.infrastructure.NodeType;
import it.uniroma2.edf.EDFLogger;
import it.uniroma2.edf.JobGraphUtils;
import it.uniroma2.edf.am.execute.GlobalActuator;
import it.uniroma2.edf.am.monitor.ApplicationMonitor;
import it.uniroma2.edf.am.plan.ReconfigurationManager;
import it.uniroma2.edf.metrics.EDFlinkStatistics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.EDFOptions;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.shaded.netty4.io.netty.handler.logging.LogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;


public class EDFlinkApplicationManager extends ApplicationManager implements Runnable {

	protected JobGraph jobGraph;
	protected Dispatcher dispatcher;
	protected Configuration config;
	protected Map<Operator, OperatorManager> operatorManagers;
	protected Map<Operator, EDFlinkOperatorManager> EDFlinkOperatorManagers;
	protected HashMap<String, OperatorManager> perOperatorNameManagers;

	//map of every Vertex associated with the resType list of the Slots in which they are actually scheduled
	protected HashMap<JobVertexID, ArrayList<Integer>> currentDeployedSlotsResTypes;

	protected Map<Operator, Reconfiguration> reconfRequests;

	protected ApplicationMonitor appMonitor = null;
	protected GlobalActuator globalActuator;
	protected ReconfigurationManager reconfManager;
	protected Map<String, Integer> request = new HashMap<>();
	protected Map<String, JobVertexID> perOperatorNameID = new HashMap<>();

	AtomicInteger deployedCounter = new AtomicInteger();

	private int amInterval;
	private int roundsBetweenPlanning;

	//Simulation attr
	private final double LATENCY_SLO;

	private Logger logger = LoggerFactory.getLogger(Simulation.class);

	private double wSLO;
	private double wReconf;
	private double wRes;

	private boolean isReconfigured = false;

	private double iterationCost;

	private EDFlinkStatistics statistics;


	public EDFlinkApplicationManager(Configuration configuration, JobGraph jobGraph, Dispatcher dispatcher,
									 Application application, Map<Operator, EDFlinkOperatorManager> operatorManagers,
									 double sloLatency) {
		super(application, sloLatency);

		config = configuration;
		this.jobGraph = jobGraph;
		this.dispatcher = dispatcher;
		this.EDFlinkOperatorManagers = operatorManagers;

		this.amInterval = config.getInteger(EDFOptions.AM_INTERVAL_SECS);
		this.roundsBetweenPlanning = config.getInteger(EDFOptions.AM_ROUNDS_BEFORE_PLANNING);

		EDFLogger.log("EDF: AM Interval: "+ amInterval+", OM Interval: "+
				config.getLong(EDFOptions.EDF_OM_INTERVAL_SECS)+", Rounds Before Planning: " + roundsBetweenPlanning,
			LogLevel.INFO, it.uniroma2.edf.am.ApplicationManager.class);

		this.appMonitor = new ApplicationMonitor(jobGraph, configuration);
		this.globalActuator = new GlobalActuator();
		this.reconfManager = new ReconfigurationManager();
		this.statistics = new EDFlinkStatistics();

		//initializing deployedCounters around JobVertexes (not dependent by Source/Sink exclusion from App Operators)
		for (JobVertex vertex: jobGraph.getVerticesSortedTopologicallyFromSources()) {
			vertex.setDeployedCounter(deployedCounter, jobGraph.getNumberOfVertices());
			perOperatorNameID.put(vertex.getName(), vertex.getID());
		}
		//initializing current resTypes with initial desired, that will be actual
		this.currentDeployedSlotsResTypes = new HashMap<>(jobGraph.getTaskResTypes());

		startOperatorManagers();

		this.LATENCY_SLO = sloLatency;
		it.uniroma2.dspsim.Configuration conf = it.uniroma2.dspsim.Configuration.getInstance();
		this.wSLO = conf.getDouble(ConfigurationKeys.RL_OM_SLO_WEIGHT_KEY, 0.33);
		this.wReconf = conf.getDouble(ConfigurationKeys.RL_OM_RECONFIG_WEIGHT_KEY, 0.33);
		this.wRes = conf.getDouble(ConfigurationKeys.RL_OM_RESOURCES_WEIGHT_KEY, 0.33);

		logger.info("SLO latency: {}", LATENCY_SLO);

	}

	@Override
	protected Map<Operator, Reconfiguration> plan(Map<OperatorManager, OMRequest> map, Map<Operator, OMMonitoringInfo> map1) {
		return null;
	}

	protected void initialize()
	{
		EDFLogger.log("EDFlinkAM: INITIALIZED!", LogLevel.INFO, EDFlinkApplicationManager.class);
	}

	@Override
	public void run() {

		initialize();
		int round = 0;

		while (true) {
			try {
				Thread.sleep(amInterval*1000);
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

			double endToEndLatency = monitor2();//use AppMonitor

			if ((round % roundsBetweenPlanning) == 0) {
				iterationCost = 0.0;

				analyze(endToEndLatency); //calculate costs
				//FOR EXPERIMENTS
				Map<Operator, Reconfiguration> reconfRequests = plan(); //take requests
				//Map<Operator, Reconfiguration> reconfRequests = new HashMap<>();
				/*
				this.reconfRequests = reconfRequests;
				reconfRequests.forEach((op,req) -> EDFLogger.log("EDF: PLAN - reconfigurations : "+req.toString(),
					LogLevel.INFO, EDFlinkApplicationManager.class)); //print requests taken

				 */
				/*
				for (JobVertex vertex: JobGraphUtils.listSortedTopologicallyOperators(jobGraph, true, true)) {
					Operator op = perOperatorNameManagers.get(vertex.getName()).getOperator();
					EDFlinkOperatorManagers.get(op).notifyReconfigured(); //unblock EDFlinkOMs
				}

				 */
				//EDFLogger.log("AM: NOTIFIED", LogLevel.INFO, EDFlinkApplicationManager.class);
				execute(reconfRequests);

				if (isReconfigured) {
					iterationCost += this.wReconf;
					statistics.updateReconfigurations(1);

				}
				statistics.updateAvgCost(iterationCost);
				statistics.dumpCost(iterationCost);
				int[] globalDeployment = application.computeGlobalDeployment();
				for (int i = 0; i < globalDeployment.length; i++) {
					statistics.updateDeployedInstances(i, globalDeployment[i]);
				}
			}

			if ((round % 5)==0){
				statistics.dumpStats();
			}
		}
	}

	protected void startOperatorManagers(){
		this.perOperatorNameManagers = new HashMap<>();
		for (Map.Entry<Operator, EDFlinkOperatorManager> om: EDFlinkOperatorManagers.entrySet()) {
			new Thread(om.getValue()).start();
			this.perOperatorNameManagers.put(om.getKey().getName(), om.getValue().getWrappedOM());
		}
	}

	protected double monitor() {
		double endToEndLatency = 0.0;
		for (JobVertex vertex: jobGraph.getVerticesSortedTopologicallyFromSources()){
			ArrayList<Integer> resTypes = vertex.getDeployedSlotsResTypes();
			int i=1;
			for (int resType: resTypes)
				EDFLogger.log("EDF: Vertex "+ vertex.getName()+" Subtask "+ i+ " deployedResType " + resType,
					LogLevel.INFO, EDFlinkApplicationManager.class);
		}

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
			double operatorIr = appMonitor.getOperatorInputRate(vertex.getName(), vertex.getParallelism());
			//double appIr = appMonitor.getApplicationInputRate();
			double operatorLatency = appMonitor.getAvgLatencyUpToOperator(vertex);
			double avgOperatorLatency = appMonitor.getAvgOperatorLatency(vertex);
			//double processingTime = appMonitor.getAvgOperatorProcessingTime(vertex.getName());
			EDFLogger.log("EDF: operator Input Rate: " + operatorIr, LogLevel.INFO, it.uniroma2.edf.am.ApplicationManager.class);
			//EDFLogger.log("EDF: application Input Rate: " + appIr, LogLevel.INFO, it.uniroma2.edf.am.ApplicationManager.class);
			EDFLogger.log("EDF: operatorLatency: " + operatorLatency, LogLevel.INFO, it.uniroma2.edf.am.ApplicationManager.class);
			EDFLogger.log("EDF: avgOperatorLatency: " + avgOperatorLatency, LogLevel.INFO, it.uniroma2.edf.am.ApplicationManager.class);
			//EDFLogger.log("EDF: processingTime: " + processingTime, LogLevel.INFO, it.uniroma2.edf.am.ApplicationManager.class);
			//EDFLogger.log("EDF: avgLatency + processingTime: " + (processingTime+avgOperatorLatency), LogLevel.INFO, it.uniroma2.edf.am.ApplicationManager.class);

			//Latencies print for experimentation
			endToEndLatency = appMonitor.endToEndLatency();
			EDFLogger.log("EDF: Simulation-Like EndToEndLatency: " + endToEndLatency,
				LogLevel.INFO, it.uniroma2.edf.am.ApplicationManager.class);
		}
		return endToEndLatency;
	}

	protected double monitor2() {
		//Latencies print for experimentation
		double endToEndLatency = appMonitor.endToEndLatency() / 1000;
		EDFLogger.log("EDF: Simulation-Like EndToEndLatency: " + endToEndLatency,
			LogLevel.INFO, it.uniroma2.edf.am.ApplicationManager.class);


		String usages = "";
		String irs = "";
		String replicas = "";

		for (JobVertex vertex: JobGraphUtils.listSortedTopologicallyOperators(jobGraph, true, true)){
			int actualPar = vertex.getParallelism();
			Double[] mon= appMonitor.getOperatorIRandUsage(vertex.getName(),actualPar);
			usages += mon[1];
			usages += ",";
			irs += mon[0];
			irs += ",";
			replicas += actualPar;
			replicas += ",";
		}
		statistics.dumpUsagesAndIr(usages, irs);
		statistics.dumpReplicas(replicas);
		statistics.dumpLatency(String.valueOf(endToEndLatency));

		return endToEndLatency;
	}

	//(not dependent by Source/Sink exclusion from App Operators)
	protected void analyze(double endToEndLatency) {
		EDFLogger.log("AM: ANALYZE - parallelism: " +jobGraph.getVerticesAsArray()[1].getParallelism(), LogLevel.INFO, EDFlinkApplicationManager.class);
		//LOG.info("ANALYZE - parallelism: " +jobGraph.getVerticesAsArray()[2].getParallelism());
		boolean sloViolated = false;
		if (endToEndLatency > LATENCY_SLO) {
			sloViolated = true;
			statistics.updateViolations(1);
			iterationCost += this.wSLO;
		}
		double deploymentCost = application.computeDeploymentCost();
		statistics.updateResCost(deploymentCost);
		double cRes = (deploymentCost / application.computeMaxDeploymentCost());
		iterationCost += cRes * this.wRes;
		EDFLogger.log("EDF: ANALYZE - Latency SLO violation: "+sloViolated+", deployment cost: "+deploymentCost,
			LogLevel.INFO, EDFlinkApplicationManager.class);
	}

	//ACCEPT ALL REQUESTS
	protected Map<Operator, Reconfiguration> plan(){
		HashMap<OperatorManager, OMRequest> requests =  pickOMRequests(); //prendi le richieste calcolate dagli OM
		//return acceptAll(requests); //unsa una politica "ACCETTALE TUTTE"
		Map<Operator, Reconfiguration> acceptedRequests = reconfManager.acceptRequests(requests);
		reconfRequests = acceptedRequests;
		reconfRequests.forEach((op,req) -> EDFLogger.log("EDF: PLAN - reconfigurations : "+req.toString(),
			LogLevel.INFO, EDFlinkApplicationManager.class)); //print requests taken
		if (!acceptedRequests.isEmpty())
			reconfManager.fillDesiredSchedulingReconf(acceptedRequests, jobGraph, request, perOperatorNameID);
		return acceptedRequests;
	}

	protected long execute(Map<Operator, Reconfiguration> reconfigurations){
		long start = System.currentTimeMillis();
		if (reconfigurations.isEmpty()){
			EDFLogger.log("EDF: EXECUTE - no reconf, skipping execution", LogLevel.INFO, EDFlinkApplicationManager.class);
			isReconfigured = false;
			for (JobVertex vertex: JobGraphUtils.listSortedTopologicallyOperators(jobGraph, true, true)) {
				Operator op = perOperatorNameManagers.get(vertex.getName()).getOperator();
				EDFlinkOperatorManagers.get(op).notifyReconfigured();
			}
			return (System.currentTimeMillis() - start);
		}
		//prepare operator to scale request list, and desired resType list
		deployedCounter.set(0);
		globalActuator.rescale(this.dispatcher, this.jobGraph, request);
		//wait until Deployed Tasks notify their deployment
		waitForTasksDeployment();
		deployedCounter.set(0);
		reconfigureOperators();
		isReconfigured = true;

		return (System.currentTimeMillis() - start);
	}

	protected void reconfigureOperators() {
		HashMap<JobVertexID, ArrayList<Integer>> overallDesResTypes = jobGraph.getTaskResTypes();
		boolean desReconf = true;

		//JobGraphUtils.listSortedTopologicallyOperators() if SOURCE AND SINKS INCLUDED
		for (JobVertex vertex: JobGraphUtils.listSortedTopologicallyOperators(jobGraph, true, true)) {
			//ResTypes the current Vertex is deployed on
			ArrayList<Integer> actualResTypes = vertex.getDeployedSlotsResTypes();
			//ResTypes the current Vertex should be deployed on this iteration
			ArrayList<Integer> desiredResTypes = overallDesResTypes.get(vertex.getID());
			EDFLogger.log("EDF: EXECUTE Reconfigure - la Riconfigurazione desiderata per il Vertex "+vertex.getName()+
				"è "+ desiredResTypes.toString()+" mentre quella effettuata è "+actualResTypes.toString(), LogLevel.INFO,
				EDFlinkApplicationManager.class);

			if (CollectionUtils.subtract(new ArrayList<>(desiredResTypes), actualResTypes).isEmpty()) {
				EDFLogger.log("EDF: la riconfigurazione del vertex " + vertex.getName() + " desiderata è stata applicata", LogLevel.INFO, EDFlinkApplicationManager.class);
				statistics.updateDesOpReconf(1);
			} else {
				EDFLogger.log("EDF: la riconfigurazione del vertex " + vertex.getName() + " desiderata NON stata applicata", LogLevel.INFO, EDFlinkApplicationManager.class);
				desReconf = false;
				misconfigurationStats(vertex);
			}

			Operator currOperator = perOperatorNameManagers.get(vertex.getName()).getOperator();
			NodeType[] oldNodeTypes = currOperator.getInstances().toArray(new NodeType[currOperator.getInstances().size()]);
			currOperator.reconfigure(Reconfiguration.scaleIn(oldNodeTypes));

			NodeType[] newNodeTypes = new NodeType[actualResTypes.size()];
			int i=0;
			for (int newNodeTypeIndex: actualResTypes){
				newNodeTypes[i] = ComputingInfrastructure.getInfrastructure().getNodeTypes()[newNodeTypeIndex];
				i++;
			}
			currOperator.reconfigure((Reconfiguration.scaleOut(newNodeTypes)));
			//notify
			EDFlinkOperatorManagers.get(currOperator).notifyReconfigured();
			//updating current deployed res types for this vertex
			currentDeployedSlotsResTypes.put(vertex.getID(), new ArrayList<>(actualResTypes));
			EDFLogger.log("EDF: EXECUTE - Lista dei NodeTypes riconfigurati per Operator "+currOperator.getName()
				+": "+ Arrays.toString(currOperator.getCurrentDeployment()), LogLevel.INFO, EDFlinkApplicationManager.class);
		}
		//update desired for actual. No deployment order should change in already existing subtasks
		jobGraph.setTaskResTypes(new HashMap<>(currentDeployedSlotsResTypes));
		/*
		EDFLogger.log("EDF: EXECUTE - Lista dei resTypes desiderati per il giro successivo: "+jobGraph.getTaskResTypes().toString(),
			LogLevel.INFO, EDFlinkApplicationManager.class);
		 */
		if (desReconf) statistics.updateDesReconf(1);
	}

	//RICAVA LE RICHIESTE DAGLI EDFLINKOM
	protected HashMap<OperatorManager, OMRequest> pickOMRequests(){
		HashMap<OperatorManager, OMRequest> omRequests = new HashMap<>();
		for (Map.Entry<Operator, EDFlinkOperatorManager> entry : EDFlinkOperatorManagers.entrySet()){
			OMRequest request = entry.getValue().getReconfRequest();
			omRequests.put(entry.getValue().getWrappedOM(), request);
		}
		return omRequests;
	}

	public void waitForTasksDeployment() {
		while (deployedCounter.get() != jobGraph.getNumberOfVertices()) {
			synchronized (deployedCounter) {
				try {
					deployedCounter.wait();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					EDFLogger.log("Thread interrupted " + e.getMessage(), LogLevel.ERROR, EDFlinkApplicationManager.class);
				}
			}
		}
	}

	private void misconfigurationStats(JobVertex vertex){
		statistics.updateOpMisconf(1);
		OperatorManager om;
		//if vertex is OM managed (not source/sink)
		if ((om = perOperatorNameManagers.get(vertex.getName())) != null) {
			//if vertex is object of reconfiguration request
			if (reconfRequests.get(om.getOperator()) != null) {
				//if vertex reconfiguration is up-scaling
				if (reconfRequests.get(om.getOperator()).getInstancesToAdd() != null) {
					//if up-scaled task is misconfigured
					if (reconfRequests.get(om.getOperator()).getInstancesToAdd()[0].getIndex() !=
						vertex.getDeployedSlotsResTypes().get(vertex.getParallelism() - 1))
						statistics.updateNewPlacementMisconf(1);
				}
			}
		}
	}

	protected void close()
	{
		EDFLogger.log("AM: closed", LogLevel.INFO, EDFlinkApplicationManager.class);
		appMonitor.close();
	}
}
