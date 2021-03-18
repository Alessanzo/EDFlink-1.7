package it.uniroma2.edf.om;

import it.uniroma2.dspsim.dsp.Operator;
import it.uniroma2.dspsim.dsp.Reconfiguration;
import it.uniroma2.dspsim.dsp.edf.om.OMMonitoringInfo;
import it.uniroma2.dspsim.dsp.edf.om.OperatorManager;
import it.uniroma2.dspsim.dsp.edf.om.request.OMRequest;
import it.uniroma2.dspsim.dsp.edf.om.request.QBasedReconfigurationScore;
import it.uniroma2.dspsim.dsp.edf.om.request.RewardBasedOMRequest;
import it.uniroma2.dspsim.infrastructure.ComputingInfrastructure;
import it.uniroma2.dspsim.infrastructure.NodeType;
import it.uniroma2.edf.utils.EDFLogger;
import it.uniroma2.edf.am.HEDFlinkApplicationManager;
import it.uniroma2.edf.monitor.ApplicationMonitorOld;
import it.uniroma2.edf.monitor.OperatorMonitor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.EDFOptions;
import org.apache.flink.shaded.netty4.io.netty.handler.logging.LogLevel;

/*Inferior level control cycle thread that has the responsibility to monitor operator data and interact with
* scaling policies to calculate reconfiguration requests. Then it shares them with HEDFlinkAM. Wraps Operator Manager
*  and uses it to calculate rescaling through its policy*/
public class HEDFlinkOperatorManager implements Runnable{

	protected OperatorManager wrappedOM; //policies hierarchy class wrapped
	protected ApplicationMonitorOld appMonitor;
	protected OperatorMonitor opMonitor;
	protected OMRequest reconfRequest = null;
	protected long omInterval = 10;

	boolean recofigured = false;

	public HEDFlinkOperatorManager(OperatorManager operatorManager, OperatorMonitor opMonitor, Configuration configuration) {
		this.wrappedOM = operatorManager;
		this.opMonitor = opMonitor;
		this.omInterval = configuration.getLong(EDFOptions.EDF_OM_INTERVAL_SECS);
	}


	@Override
	public void run() {
		String reconf;
		while (true) {
			try {
				Thread.sleep(omInterval*1000);
			} catch (InterruptedException e) {
			}
			//operator monitor input rate and cpu usage
			OMMonitoringInfo monitoringInfo = getIrAndUtilization(wrappedOM.getOperator());
			//interaction with HEDFlink Operator Manager to calculate rescaling request
			this.reconfRequest = wrappedOM.pickReconfigurationRequest(monitoringInfo);
			reconf = reconfRequest.getRequestedReconfiguration().toString();

			EDFLogger.log("HEDF: HEDFlinkOM for Operator "+getWrappedOM().getOperator().getName()+
					" monitored Input Rate: "+monitoringInfo.getInputRate()+ " and CPUUsage: "
					+ monitoringInfo.getCpuUtilization()+ " and decided this Reconfiguration Request: "+ reconf
				, LogLevel.INFO, HEDFlinkOperatorManager.class);
			//waits for Operator info to be reconfigured by EDFlinkAM after rescaling, before starting with a new cycle
			waitReconfigured();

			EDFLogger.log("HEDF: "+ getWrappedOM().getOperator().getName()+" reconfiguration completed"
				, LogLevel.INFO, HEDFlinkOperatorManager.class);
		}
	}

	//synchronization with EDFlinkAM to wait for reconfiguration completion and Operator structure update
	public void waitReconfigured() {
		while (!recofigured) {
			synchronized (this) {
				try {
					wait();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					EDFLogger.log("Thread interrupted " + e.getMessage(), LogLevel.ERROR, HEDFlinkApplicationManager.class);
				}
			}
		}
		recofigured = false;
	}

	//interaction with Operator Monitor
	public OMMonitoringInfo getIrAndUtilization(Operator operator){
		String operatorName = operator.getName();
		int currentParallelism = operator.getInstances().size();
		return opMonitor.getOMMonitoringInfo(operatorName, currentParallelism);
	}

	//method used by HEDFlinkAM to fetch posted reconfiguration request
	public OMRequest getReconfRequest() {
		OMRequest newRequest;
		if (reconfRequest != null)
			newRequest = reconfRequest;
		else
			newRequest = new RewardBasedOMRequest(Reconfiguration.doNothing(), new QBasedReconfigurationScore(0D),
				new QBasedReconfigurationScore(0D));
		reconfRequest = null;
		return newRequest;
	}

	//method used by HEDFlinkAM to notify completed reconfiguration
	public void notifyReconfigured() {
		recofigured = true;
		synchronized (this) {
			notify();
		}
	}

	public OperatorManager getWrappedOM() {
		return wrappedOM;
	}

}
