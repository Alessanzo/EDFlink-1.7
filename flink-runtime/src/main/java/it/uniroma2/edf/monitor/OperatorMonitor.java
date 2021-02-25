package it.uniroma2.edf.monitor;

import it.uniroma2.dspsim.dsp.edf.om.OMMonitoringInfo;
import it.uniroma2.edf.monitor.Monitor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;

public class OperatorMonitor extends Monitor {
	public OperatorMonitor(JobGraph jobGraph, Configuration configuration) {
		super(jobGraph, configuration);
	}


	public OMMonitoringInfo getOMMonitoringInfo(String operatorName, int currentParallelism) {
		Double[] irAndUsage = getOperatorIRandUsage(operatorName, currentParallelism);
		OMMonitoringInfo monitoringInfo = new OMMonitoringInfo();
		monitoringInfo.setInputRate(irAndUsage[0]);
		monitoringInfo.setCpuUtilization(irAndUsage[1]);
		return monitoringInfo;
	}
}
