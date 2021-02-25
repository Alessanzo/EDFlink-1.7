package it.uniroma2.edf;

import it.uniroma2.dspsim.Configuration;
import it.uniroma2.dspsim.ConfigurationKeys;
import it.uniroma2.dspsim.dsp.Application;
import it.uniroma2.dspsim.dsp.Operator;
import it.uniroma2.dspsim.dsp.edf.am.ApplicationManager;
import it.uniroma2.dspsim.dsp.edf.om.OperatorManager;
import it.uniroma2.dspsim.dsp.edf.om.OperatorManagerType;
import it.uniroma2.dspsim.infrastructure.ComputingInfrastructure;
import it.uniroma2.dspsim.infrastructure.NodeType;
import it.uniroma2.edf.am.HEDFlinkApplicationManager;
import it.uniroma2.edf.monitor.OperatorMonitor;
import it.uniroma2.edf.utils.EDFLogger;
import it.uniroma2.edf.om.HEDFlinkOperator;
import it.uniroma2.edf.om.HEDFlinkOperatorManager;
import it.uniroma2.edf.om.HEDFlinkOperatorManagerFactory;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.shaded.netty4.io.netty.handler.logging.LogLevel;

import java.util.*;

/*Class that instanciates HEDFlink ApplicationManager and OperatorManagers according to configuration
 parameters and starts their cycle threads.*/
public class HEDFlink {

	private Application application;
	private Map<Operator, OperatorManager> operatorManagers; //Scaling Policies Library Classes wrapped and managed by HEDFlinkOperatorManagers
	private HashMap<Operator, HEDFlinkOperatorManager> edFlinkOperatorManagers; //Providing Thread Cycle to Policies Operator Managers

	public HEDFlink(Application application, org.apache.flink.configuration.Configuration configuration
		, JobGraph jobGraph, Dispatcher dispatcher) {
		this.application = application;

		final List<Operator> operators = application.getOperators();
		Configuration conf = Configuration.getInstance();
		double latencySLO = conf.getDouble(ConfigurationKeys.SLO_LATENCY_KEY, 0.100);
		EDFLogger.log("HEDF - latencySLO: "+latencySLO, LogLevel.INFO, HEDFlink.class);

		final int numOperators = operators.size();

		operatorManagers = new HashMap<>(numOperators);
		edFlinkOperatorManagers = new HashMap<>(numOperators);
		for (Operator op : operators) { //each Application Operator is bound to a Manager
			OperatorMonitor opMonitor = new OperatorMonitor(jobGraph, configuration);
			((HEDFlinkOperator) op).setOpMonitor(opMonitor);
			//instantiation of the original OM
			OperatorManager om = newOperatorManager(op, conf);
			//instantiation of the wrappedOM
			HEDFlinkOperatorManager edFlinkOM = newHEDFlinkOperatorManager(om, opMonitor, configuration);
			operatorManagers.put(op, om);
			edFlinkOperatorManagers.put(op, edFlinkOM);
		}
		//instantiation of the AM passing wrappedOM's
		ApplicationManager appManager = newHEDFlinkApplicationManager(configuration, jobGraph, dispatcher, latencySLO);
		//AM start
		new Thread((Runnable) appManager).start();
	}

	protected HEDFlinkOperatorManager newHEDFlinkOperatorManager(OperatorManager om, OperatorMonitor opMonitor,
																org.apache.flink.configuration.Configuration configuration) {
		return new HEDFlinkOperatorManager(om, opMonitor, configuration);
	}

	//Infrastructure configuration invoked in ClusterEntripoint.startCluster()
	public static void initialize() {
		//parsing config.properties from Flink Configuration Directory
		Configuration conf = HEDFlinkConfiguration.getEDFlinkConfInstance();
		String filepath = System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR) + "/config.properties";
		conf.parseConfigurationFile(filepath);

		//getting # of node types and simulation CPU speedups for initializing EDF Infrastructure
		int nodeTypesNum = conf.getInteger(ConfigurationKeys.NODE_TYPES_NUMBER_KEY, 3);
		String[] confCpuSpeedups = conf.getString("simulation.cpu.speedups", "0.7,1.0,1.3,0.9,1.7,0.8,1.8,2.0,1.65,1.5").split(",");
		double[] nodeCpuSpeedups = new double[nodeTypesNum];
		//speedups specified at least as many as # of res types
		if (nodeCpuSpeedups.length != 0 && confCpuSpeedups.length >= nodeTypesNum){
			try {
				for (int i=0;i<nodeCpuSpeedups.length;i++)
					nodeCpuSpeedups[i] = Double.parseDouble(confCpuSpeedups[i]);
			} catch (NumberFormatException e){
				nodeCpuSpeedups = new double[]{0.7, 1.0, 1.3, 0.9, 1.7, 0.8, 1.8, 2.0, 1.65, 1.5};
			}
		}
		ComputingInfrastructure.initCustomInfrastructure(nodeCpuSpeedups, nodeTypesNum);
		NodeType[] nodeTypes = ComputingInfrastructure.getInfrastructure().getNodeTypes();
		Arrays.stream(nodeTypes).forEach(nodeType -> EDFLogger.log("HEDF: Node with Type " + nodeType.getIndex()
			+ ", speedup "+nodeType.getCpuSpeedup() + ", cost "+ nodeType.getCost(), LogLevel.INFO, HEDFlink.class));
		EDFLogger.log("HEDF: Starting Restype: "+ HEDFlinkConfiguration.getEDFlinkConfInstance().getInteger("node.types.starting", 1), LogLevel.INFO,
			HEDFlink.class);


	}

	public ApplicationManager newHEDFlinkApplicationManager(org.apache.flink.configuration.Configuration configuration
		, JobGraph jobGraph, Dispatcher dispatcher, double sloLatency) {
		return new HEDFlinkApplicationManager(configuration, jobGraph, dispatcher, application, edFlinkOperatorManagers, sloLatency);
	}


	//Create Operator Manager according to che Policy specified in config.properties
	public OperatorManager newOperatorManager(Operator op, Configuration configuration) {
		String omType = configuration.getString("edf.om.type", "qlearning");
		EDFLogger.log("HEDF: Policy used by Operator Managers : " + omType, LogLevel.INFO,HEDFlink.class);
		OperatorManagerType operatorManagerType = OperatorManagerType.fromString(omType);
		return HEDFlinkOperatorManagerFactory.createOperatorManager(operatorManagerType, op);
	}

}
