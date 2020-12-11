package it.uniroma2.edf.am;

import it.uniroma2.dspsim.Configuration;
import it.uniroma2.dspsim.ConfigurationKeys;
import it.uniroma2.dspsim.dsp.Operator;
import it.uniroma2.dspsim.dsp.Reconfiguration;
import it.uniroma2.dspsim.dsp.edf.om.OMMonitoringInfo;
import it.uniroma2.dspsim.dsp.edf.om.OperatorManager;
import it.uniroma2.dspsim.dsp.edf.om.request.OMRequest;
import it.uniroma2.dspsim.dsp.edf.om.request.QBasedReconfigurationScore;
import it.uniroma2.dspsim.dsp.edf.om.request.ReconfigurationScore;
import it.uniroma2.dspsim.dsp.edf.om.request.RewardBasedOMRequest;
import it.uniroma2.dspsim.infrastructure.ComputingInfrastructure;
import it.uniroma2.dspsim.infrastructure.NodeType;
import it.uniroma2.edf.EDFLogger;
import it.uniroma2.edf.am.monitor.ApplicationMonitor;
import org.apache.flink.shaded.netty4.io.netty.handler.logging.LogLevel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.OptionalDouble;

public class EDFlinkOperatorManager implements Runnable{

	protected OperatorManager wrappedOM;
	protected ApplicationMonitor appMonitor;
	protected OMRequest reconfRequest = null;

	boolean recofigured = false;

	public EDFlinkOperatorManager(OperatorManager operatorManager, ApplicationMonitor appMonitor) {
		this.wrappedOM = operatorManager;
		this.appMonitor = appMonitor;
	}

	public EDFlinkOperatorManager(OperatorManager operatorManager) {
		this.wrappedOM = operatorManager;
	}


	@Override
	public void run() {
		String reconf;
		while (true) {
			try {
				//Thread.sleep(amInterval*1000);
				Thread.sleep(10000);
			} catch (InterruptedException e) {
			}
			int currentParallelism = wrappedOM.getOperator().getInstances().size();
			getIrAndUtilization(wrappedOM.getOperator());
			double operatorInputRate = appMonitor.getOperatorInputRate(wrappedOM.getOperator().getName(), currentParallelism);
			double cpuUsage = appMonitor.getOperatorCpuUsage(wrappedOM.getOperator().getName(), currentParallelism);

			EDFLogger.log("EDF: EDFLINKOM monitored for Operator "+getWrappedOM().getOperator().getName()+
				" with current parallelism " + currentParallelism + " this Input Rate: "+operatorInputRate +
					" and this CPUUsage: "+ cpuUsage, LogLevel.INFO, EDFlinkOperatorManager.class);
			//double cpuUsage = appMonitor.getOperatorCpuUsage(wrappedOM.getOperator());
			final double u = wrappedOM.getOperator().utilization(operatorInputRate);
			OMMonitoringInfo monitoringInfo = new OMMonitoringInfo();
			monitoringInfo.setInputRate(operatorInputRate);
			monitoringInfo.setCpuUtilization(u);
			this.reconfRequest = wrappedOM.pickReconfigurationRequest(monitoringInfo);
			reconf = reconfRequest.getRequestedReconfiguration().toString();
			EDFLogger.log("EDF: EDFLINKOM for Operator "+getWrappedOM().getOperator().getName()+
					" decided this Reconfiguration Request: "+ reconf
				, LogLevel.INFO, EDFlinkOperatorManager.class);
			waitReconfigured();
			EDFLogger.log("EDF: EDFLINKOM waited for reconfiguration", LogLevel.INFO, EDFlinkOperatorManager.class);
		}
	}

	public void waitReconfigured() {
		while (!recofigured) {
			synchronized (this) {
				try {
					wait();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					EDFLogger.log("Thread interrupted " + e.getMessage(), LogLevel.ERROR, EDFlinkApplicationManager.class);
				}
			}
		}
		recofigured = false;
	}

	public OMMonitoringInfo getIrAndUtilization(Operator operator){
		String operatorName = operator.getName();
		int currentParallelism = operator.getInstances().size();
		double operatorInputRate = 0;
		double operatorCpuUsage = 0;

		ArrayList<Double> subtaskInputRates = appMonitor.getSubtaskInputRates(operatorName, currentParallelism);
		ArrayList<Double> subtaskCpuUsages = appMonitor.getSubtaskCpuUsages(operatorName, currentParallelism);
 		ArrayList<Double> subtaskIRFractions = new ArrayList<>();

		for (double subtaskIR: subtaskInputRates)
			operatorInputRate+=subtaskIR;

		String mode = Configuration.getInstance().getString(
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

		EDFLogger.log("EDF: EDFLINKOM with new method monitored for Operator"+operatorName+
			" with current parallelism " + currentParallelism + " this Input Rate: "+operatorInputRate +
			" and this CPUUsage: "+ operatorCpuUsage, LogLevel.INFO, EDFlinkOperatorManager.class);
		return monitoringInfo;
	}

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

	public void notifyReconfigured() {
		recofigured = true;
		synchronized (this) {
			notify();
		}
	}

	public OperatorManager getWrappedOM() {
		return wrappedOM;
	}

	public void run2(){
		int i = 0;
		String reconf = "(do nothing)";
		while (true) {
			try {
				//Thread.sleep(amInterval*1000);
				Thread.sleep(10000);
			} catch (InterruptedException e) {
			}
			double operatorInputRate = appMonitor.getOperatorInputRate(wrappedOM.getOperator().getName());
			EDFLogger.log("EDF: EDFLINKOM monitored for Operator "+getWrappedOM().getOperator().getName()+
				" this Input Rate: "+operatorInputRate, LogLevel.INFO, EDFlinkOperatorManager.class);
			final double u = wrappedOM.getOperator().utilization(operatorInputRate);
			OMMonitoringInfo monitoringInfo = new OMMonitoringInfo();
			monitoringInfo.setInputRate(operatorInputRate);
			monitoringInfo.setCpuUtilization(u);

			if (reconf.equals("(do nothing)") || i<=1){
				OMRequest reconfReq = wrappedOM.pickReconfigurationRequest(monitoringInfo);
				NodeType[] nodeTypes = reconfReq.getRequestedReconfiguration().getInstancesToAdd();
				NodeType[] enlargedNodeTypes;
				if (i==0) {
					if (nodeTypes != null) {
						enlargedNodeTypes = new NodeType[]{nodeTypes[0], nodeTypes[0]};
						reconfReq = new RewardBasedOMRequest(Reconfiguration.scaleOut(enlargedNodeTypes), new QBasedReconfigurationScore(0), new QBasedReconfigurationScore(0));
					}
				}
				if (i==1) {
					enlargedNodeTypes = new NodeType[]{ComputingInfrastructure.getInfrastructure().getNodeTypes()[1]};
					reconfReq = new RewardBasedOMRequest(Reconfiguration.scaleIn(enlargedNodeTypes), new QBasedReconfigurationScore(0), new QBasedReconfigurationScore(0));
				}
				this.reconfRequest = reconfReq;
				//this.reconfRequest = wrappedOM.pickReconfigurationRequest(monitoringInfo);
				reconf = reconfRequest.getRequestedReconfiguration().toString();
				EDFLogger.log("EDF: EDFLINKOM for Operator "+getWrappedOM().getOperator().getName()+
						" decided this Reconfiguration Request: "+ reconf
					, LogLevel.INFO, EDFlinkOperatorManager.class);

				if (!reconf.equals("(do nothing)"))
					i++;
			}
			waitReconfigured();
			EDFLogger.log("EDF: EDFLINKOM waited for reconfiguration", LogLevel.INFO, EDFlinkOperatorManager.class);
		}
	}
}
