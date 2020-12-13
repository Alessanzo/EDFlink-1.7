package it.uniroma2.edf.am;

import it.uniroma2.dspsim.Configuration;
import it.uniroma2.dspsim.ConfigurationKeys;
import it.uniroma2.dspsim.dsp.Application;
import it.uniroma2.dspsim.dsp.ApplicationBuilder;
import it.uniroma2.dspsim.dsp.Operator;
import it.uniroma2.dspsim.dsp.edf.EDF;
import it.uniroma2.dspsim.dsp.edf.am.ApplicationManager;
import it.uniroma2.dspsim.dsp.edf.am.ApplicationManagerFactory;
import it.uniroma2.dspsim.dsp.edf.am.ApplicationManagerType;
import it.uniroma2.dspsim.dsp.edf.om.OperatorManager;
import it.uniroma2.dspsim.dsp.edf.om.OperatorManagerType;
import it.uniroma2.dspsim.dsp.edf.om.factory.OperatorManagerFactory;
import it.uniroma2.dspsim.dsp.queueing.MG1OperatorQueueModel;
import it.uniroma2.dspsim.infrastructure.ComputingInfrastructure;
import it.uniroma2.dspsim.utils.Tuple2;
import it.uniroma2.dspsim.utils.matrix.DoubleMatrix;
import it.uniroma2.edf.EDFLogger;
import it.uniroma2.edf.EDFlinkConfiguration;
import it.uniroma2.edf.JobGraphUtils;
import it.uniroma2.edf.am.monitor.ApplicationMonitor;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.shaded.netty4.io.netty.handler.logging.LogLevel;

import java.io.*;
import java.util.*;

public class EDFlink extends EDF {

	private Application application;
	private Map<Operator, OperatorManager> operatorManagers;
	private HashMap<Operator, EDFlinkOperatorManager> edFlinkOperatorManagers;

	public EDFlink(Application application, ApplicationMonitor appMonitor, org.apache.flink.configuration.Configuration configuration
		, JobGraph jobGraph, Dispatcher dispatcher, double sloLatency) {
		super(application, sloLatency);
		this.application = application;

		final List<Operator> operators = application.getOperators();
		Configuration conf = Configuration.getInstance();

		final int numOperators = operators.size();

		operatorManagers = new HashMap<>(numOperators);
		edFlinkOperatorManagers = new HashMap<>(numOperators);
		for (Operator op : operators) {
			//instantiation of the original OM
			OperatorManager om = newOperatorManager(op, conf);
			//instantiation of the wrappedOM
			EDFlinkOperatorManager edFlinkOM = newEDFlinkOperatorManager(om, appMonitor);
			operatorManagers.put(op, om);
			edFlinkOperatorManagers.put(op, edFlinkOM);
		}
		//instantiation of the AM passing wrappedOM's
		ApplicationManager appManager = newApplicationManager(configuration, jobGraph, dispatcher, appMonitor, sloLatency);
		//AM start
		new Thread((Runnable) appManager).start();

		//simulation.dumpConfigs();
		//simulation.dumpStats();
	}

	protected EDFlinkOperatorManager newEDFlinkOperatorManager(OperatorManager om, ApplicationMonitor appMonitor) {
		return new EDFlinkOperatorManager(om, appMonitor);
	}

	//invoked in ClusterEntripoint.startCluster()
	public static void initialize() {
		Configuration conf = EDFlinkConfiguration.getEDFlinkConfInstance();
		conf.parseDefaultConfigurationFile();
		ComputingInfrastructure.initCustomInfrastructure(
			new double[]{1.0, 0.7, 1.3, 0.9, 1.7, 0.8, 1.8, 2.0, 1.65, 1.5},
			conf.getInteger(ConfigurationKeys.NODE_TYPES_NUMBER_KEY, 3));

	}

	public ApplicationManager newApplicationManager(org.apache.flink.configuration.Configuration configuration
		, JobGraph jobGraph, Dispatcher dispatcher, ApplicationMonitor appMonitor, double sloLatency) {
		return new EDFlinkApplicationManager(configuration, jobGraph, dispatcher, application, edFlinkOperatorManagers, sloLatency, appMonitor);
	}


	@Override
	public OperatorManager newOperatorManager(Operator op, Configuration configuration) {
		String omType = configuration.getString("edf.om.type", "qlearning");
		OperatorManagerType operatorManagerType = OperatorManagerType.fromString(omType);
		return EDFlinkOperatorManagerFactory.createOperatorManager(operatorManagerType, op);
	}

}
