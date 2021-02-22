package it.uniroma2.edf.am.monitor;

import it.uniroma2.dspsim.ConfigurationKeys;
import it.uniroma2.dspsim.dsp.edf.om.OMMonitoringInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;

import java.util.ArrayList;
import java.util.Collections;

public class OperatorMonitorProva extends Monitor{
	public OperatorMonitorProva(JobGraph jobGraph, Configuration configuration) {
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
