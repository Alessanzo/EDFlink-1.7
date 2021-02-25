package it.uniroma2.edf.om;

import it.uniroma2.dspsim.dsp.Operator;
import it.uniroma2.dspsim.dsp.queueing.OperatorQueueModel;
import it.uniroma2.edf.utils.EDFLogger;
import it.uniroma2.edf.monitor.ApplicationMonitorOld;
import it.uniroma2.edf.monitor.OperatorMonitor;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.shaded.netty4.io.netty.handler.logging.LogLevel;

public class HEDFlinkOperator extends Operator {

	protected ApplicationMonitorOld appMonitor;
	protected OperatorMonitor opMonitor;
	protected JobVertex vertex;

	public HEDFlinkOperator(JobVertex jobVertex, String name,
							OperatorQueueModel queueModel, int maxParallelism) {
		super(name, queueModel, maxParallelism);
		this.vertex = jobVertex;
	}

	public void setOpMonitor(OperatorMonitor opMonitor){
		this.opMonitor = opMonitor;
	}

	@Override
	public double responseTime(double inputRate) {
		//double operatorLatency = appMonitor.getAvgOperatorLatency(vertex);
		//double procTime = appMonitor.getAvgOperatorProcessingTime(vertex);
		double operatorLatency = opMonitor.getAvgOperatorLatency(vertex);
		double procTime = opMonitor.getAvgOperatorProcessingTime(vertex);
		double opRespTime = (operatorLatency+procTime) / 1000;
		EDFLogger.log("EDF: metodo overridden invocato. Latenza operatore: "+ opRespTime
			+", SLO operatore: "+getSloRespTime(), LogLevel.INFO, HEDFlinkOperator.class);
		return opRespTime;
	}
}
