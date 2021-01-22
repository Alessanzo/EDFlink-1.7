package it.uniroma2.edf.am;

import it.uniroma2.dspsim.dsp.Operator;
import it.uniroma2.dspsim.dsp.queueing.OperatorQueueModel;
import it.uniroma2.edf.EDFLogger;
import it.uniroma2.edf.am.monitor.ApplicationMonitor;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.shaded.netty4.io.netty.handler.logging.LogLevel;

public class EDFlinkOperator extends Operator {

	protected ApplicationMonitor appMonitor;
	protected JobVertex vertex;

	public EDFlinkOperator(ApplicationMonitor appMonitor, JobVertex jobVertex, String name,
						   OperatorQueueModel queueModel, int maxParallelism) {
		super(name, queueModel, maxParallelism);
		this.appMonitor = appMonitor;
		this.vertex = jobVertex;
	}

	@Override
	public double responseTime(double inputRate) {
		double operatorLatency = appMonitor.getAvgOperatorLatency(vertex);
		double procTime = appMonitor.getAvgOperatorProcessingTime(vertex);
		double opRespTime = (operatorLatency+procTime) / 1000;
		EDFLogger.log("EDF: metodo overridden invocato. Latenza operatore: "+ opRespTime
			+", SLO operatore: "+getSloRespTime(), LogLevel.INFO, EDFlinkOperator.class);
		return opRespTime;
	}
}
