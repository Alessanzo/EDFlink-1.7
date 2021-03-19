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
import it.uniroma2.edf.monitor.ApplicationMonitor;
import it.uniroma2.edf.utils.EDFLogger;
import it.uniroma2.edf.utils.JobGraphUtils;
import it.uniroma2.edf.metrics.HEDFlinkStatistics;
import it.uniroma2.edf.om.HEDFlinkOperatorManager;
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

/*HEDFlinkAM implements the superior level cycle thread, has global vision over the Job, interacts with Flink
* scheduling, scaling, monitoring. Monitors metrics, analyzes them and calculates execution statistics, collects
* EDFlinkOMs reconfiguration requests checks them, executes rescaling. Then updates deployment informations and Operator
* structures.*/
public class HEDFlinkApplicationManager extends ApplicationManager implements Runnable {

	protected JobGraph jobGraph;
	protected Dispatcher dispatcher;
	protected Configuration config;
	//support structures
	protected Map<Operator, OperatorManager> operatorManagers;
	protected Map<Operator, HEDFlinkOperatorManager> EDFlinkOperatorManagers;
	protected HashMap<String, OperatorManager> perOperatorNameManagers;

	//map of every Vertex associated with the resType list of the Slots in which they are actually scheduled
	protected HashMap<JobVertexID, ArrayList<Integer>> currentDeployedSlotsResTypes;
	//reconfiguration requests map
	protected Map<Operator, Reconfiguration> reconfRequests;

	//components for monitoring, analyzing, planning, executing
	protected ApplicationMonitor appMonitor = null;
	private HEDFlinkStatistics statistics;
	protected GlobalActuator globalActuator;
	protected ReconfigurationManager reconfManager;

	protected Map<String, Integer> request = new HashMap<>();
	protected Map<String, JobVertexID> perOperatorNameID = new HashMap<>();

	AtomicInteger deployedCounter = new AtomicInteger();

	private int amInterval; //time between cycles
	private int roundsBetweenPlanning;

	//policies attributes
	private final double LATENCY_SLO; //service level objective latency value

	private Logger logger = LoggerFactory.getLogger(Simulation.class);
	//cost weights
	private double wSLO;
	private double wReconf;
	private double wRes;

	private boolean isReconfigured = false;

	private double iterationCost;


	public HEDFlinkApplicationManager(Configuration configuration, JobGraph jobGraph, Dispatcher dispatcher,
									  Application application, Map<Operator, HEDFlinkOperatorManager> operatorManagers,
									  double sloLatency) {
		super(application, sloLatency);

		config = configuration;
		this.jobGraph = jobGraph;
		this.dispatcher = dispatcher;
		this.EDFlinkOperatorManagers = operatorManagers;

		this.amInterval = config.getInteger(EDFOptions.AM_INTERVAL_SECS);
		this.roundsBetweenPlanning = config.getInteger(EDFOptions.AM_ROUNDS_BEFORE_PLANNING);

		EDFLogger.log("HEDF: AM Interval: "+ amInterval+", OM Interval: "+
				config.getLong(EDFOptions.EDF_OM_INTERVAL_SECS)+", Rounds Before Planning: " + roundsBetweenPlanning,
			LogLevel.INFO, HEDFlinkApplicationManager.class);

		this.appMonitor = new ApplicationMonitor(jobGraph, configuration);
		this.globalActuator = new GlobalActuator();
		this.reconfManager = new ReconfigurationManager();
		this.statistics = new HEDFlinkStatistics();

		//initializing deployedCounters around JobVertexes (not dependent by Source/Sink exclusion from App Operators)
		for (JobVertex vertex: jobGraph.getVerticesSortedTopologicallyFromSources()) {
			vertex.setDeployedCounter(deployedCounter, jobGraph.getNumberOfVertices());
			perOperatorNameID.put(vertex.getName(), vertex.getID());
		}
		//initializing current resTypes with initial desired, that will be actual
		this.currentDeployedSlotsResTypes = new HashMap<>(jobGraph.getTaskResTypes());

		startOperatorManagers();

		//initializing policies info from configuration.properties
		this.LATENCY_SLO = sloLatency;
		it.uniroma2.dspsim.Configuration conf = it.uniroma2.dspsim.Configuration.getInstance();
		this.wSLO = conf.getDouble(ConfigurationKeys.RL_OM_SLO_WEIGHT_KEY, 0.33);
		this.wReconf = conf.getDouble(ConfigurationKeys.RL_OM_RECONFIG_WEIGHT_KEY, 0.33);
		this.wRes = conf.getDouble(ConfigurationKeys.RL_OM_RESOURCES_WEIGHT_KEY, 0.33);

		logger.info("HEDF: SLO latency: {}", LATENCY_SLO);

	}

	@Override
	protected Map<Operator, Reconfiguration> plan(Map<OperatorManager, OMRequest> map, Map<Operator, OMMonitoringInfo> map1) {
		return null;
	}

	protected void initialize()
	{
		EDFLogger.log("EDFlinkAM: INITIALIZED!", LogLevel.INFO, HEDFlinkApplicationManager.class);
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
			//executing while Job is RUNNING
			CompletableFuture<JobStatus> jobStatusFuture = dispatcher.requestJobStatus(jobGraph.getJobID(), Time.seconds(3));
			try {
				//EDFLogger.log("HEDF: GETTING JOB STATUS!", LogLevel.INFO, HEDFlinkApplicationManager.class);
				JobStatus jobStatus = jobStatusFuture.get();
				if (jobStatus == JobStatus.FAILED || jobStatus == JobStatus.CANCELED || jobStatus == JobStatus.CANCELLING) {
					EDFLogger.log("HEDF: CLOSING", LogLevel.INFO, HEDFlinkApplicationManager.class);
					close();
					return;
				} else if (jobStatus == JobStatus.FINISHED) {
					EDFLogger.log("HEDF: JOB FINISHED", LogLevel.INFO, HEDFlinkApplicationManager.class);
				} else if (jobStatus != JobStatus.RUNNING) {
					EDFLogger.log("HEDF: Job Not Running... AM inhibited", LogLevel.INFO, HEDFlinkApplicationManager.class);
					continue;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}

			round = (round + 1);
			EDFLogger.log("HEDF: Round " + round, LogLevel.INFO, HEDFlinkApplicationManager.class);

			double endToEndLatency = monitor();// monitor latency in every round

			if ((round % roundsBetweenPlanning) == 0) { //planning/execution round
				iterationCost = 0.0;

				analyze(endToEndLatency); //calculate costs

				Map<Operator, Reconfiguration> reconfRequests = plan(); //take requests

				execute(reconfRequests);

				//stats updates after reconfiguration
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
		for (Map.Entry<Operator, HEDFlinkOperatorManager> om: EDFlinkOperatorManagers.entrySet()) {
			new Thread(om.getValue()).start();
			this.perOperatorNameManagers.put(om.getKey().getName(), om.getValue().getWrappedOM());
		}
	}

	//end to end latency collection through app monitor and stats update
	protected double monitor() {
		//Latencies print for experimentation
		double endToEndLatency = appMonitor.endToEndLatency() / 1000;
		EDFLogger.log("HEDF: end-to-end Latency: " + endToEndLatency,
			LogLevel.INFO, HEDFlinkApplicationManager.class);


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

	//stats and violation costs update
	protected void analyze(double endToEndLatency) {

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
		EDFLogger.log("HEDF: ANALYZE - Latency SLO violation: "+sloViolated+", deployment cost: "+deploymentCost,
			LogLevel.INFO, HEDFlinkApplicationManager.class);
	}

	//planning method
	protected Map<Operator, Reconfiguration> plan(){
		HashMap<OperatorManager, OMRequest> requests =  pickOMRequests(); //collecting reconf requests from HEDFlinkOMs
		Map<Operator, Reconfiguration> acceptedRequests = reconfManager.acceptRequests(requests);
		reconfRequests = acceptedRequests;
		reconfRequests.forEach((op,req) -> EDFLogger.log("HEDF: PLAN - reconfigurations : "+req.toString(),
			LogLevel.INFO, HEDFlinkApplicationManager.class)); //print requests taken
		//filling map used by rescaling methods with (operator-subtask num)
		if (!acceptedRequests.isEmpty())
			reconfManager.fillDesiredSchedulingReconf(acceptedRequests, jobGraph, request, perOperatorNameID);
		return acceptedRequests;
	}

	//execution method
	protected long execute(Map<Operator, Reconfiguration> reconfigurations){
		long start = System.currentTimeMillis();
		if (reconfigurations.isEmpty()){
			//EDFLogger.log("HEDF: EXECUTE - no reconf, skipping execution", LogLevel.INFO, HEDFlinkApplicationManager.class);
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
		reconfigureOperators(); //update Operator structures
		isReconfigured = true;

		return (System.currentTimeMillis() - start);
	}

	protected void reconfigureOperators() {
		HashMap<JobVertexID, ArrayList<Integer>> overallDesResTypes = jobGraph.getTaskResTypes();
		boolean desReconf = true;

		for (JobVertex vertex: JobGraphUtils.listSortedTopologicallyOperators(jobGraph, true, true)) {
			//ResTypes the current Vertex is deployed on
			ArrayList<Integer> actualResTypes = vertex.getDeployedSlotsResTypes();
			//ResTypes the current Vertex should be deployed on this iteration
			ArrayList<Integer> desiredResTypes = overallDesResTypes.get(vertex.getID());
			EDFLogger.log("HEDF: EXECUTE Reconfigure - Desired reconfiguration for vertex is "+vertex.getName()+
				"è "+ desiredResTypes.toString()+" while executed is "+actualResTypes.toString(), LogLevel.INFO,
				HEDFlinkApplicationManager.class);
			//check if vertex reconfiguration actually done is the desired one
			if (CollectionUtils.subtract(new ArrayList<>(desiredResTypes), actualResTypes).isEmpty()) {
				EDFLogger.log("HEDF: desired vertex reconf " + vertex.getName() + " has been applied", LogLevel.INFO, HEDFlinkApplicationManager.class);
				statistics.updateDesOpReconf(1);
			} else {
				EDFLogger.log("HEDF: desired vertex reconf " + vertex.getName() + " has NOT been applied", LogLevel.INFO, HEDFlinkApplicationManager.class);
				desReconf = false;
				misconfigurationStats(vertex);
			}

			//update Operator structure used by policies based on new vertex configuration obtained after scaling
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
			EDFLogger.log("HEDF: EXECUTE - Desired recofigured NodeTypes list for Operator "+currOperator.getName()
				+": "+ Arrays.toString(currOperator.getCurrentDeployment()), LogLevel.INFO, HEDFlinkApplicationManager.class);
		}
		//update desired resTypes for next reconfiguration with new deployed slot types
		jobGraph.setTaskResTypes(new HashMap<>(currentDeployedSlotsResTypes));
		if (desReconf) statistics.updateDesReconf(1);
	}

	//fetch HEDFlinkOMs rescaling requests
	protected HashMap<OperatorManager, OMRequest> pickOMRequests(){
		HashMap<OperatorManager, OMRequest> omRequests = new HashMap<>();
		for (Map.Entry<Operator, HEDFlinkOperatorManager> entry : EDFlinkOperatorManagers.entrySet()){
			OMRequest request = entry.getValue().getReconfRequest();
			omRequests.put(entry.getValue().getWrappedOM(), request);
		}
		return omRequests;
	}

	//wait for task deployment after scaling to get to know on which resType they have been depolyed, and update structures
	public void waitForTasksDeployment() {
		while (deployedCounter.get() != jobGraph.getNumberOfVertices()) {
			synchronized (deployedCounter) {
				try {
					deployedCounter.wait();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					EDFLogger.log("Thread interrupted " + e.getMessage(), LogLevel.ERROR, HEDFlinkApplicationManager.class);
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
		EDFLogger.log("AM: closed", LogLevel.INFO, HEDFlinkApplicationManager.class);
		appMonitor.close();
	}
}
