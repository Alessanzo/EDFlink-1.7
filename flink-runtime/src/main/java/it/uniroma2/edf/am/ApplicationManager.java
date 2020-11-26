package it.uniroma2.edf.am;

//import it.uniroma2.edf.am.monitor.ApplicationMonitor;
import it.uniroma2.edf.EDFLogger;
import it.uniroma2.edf.JobGraphUtils;
import it.uniroma2.edf.am.execute.GlobalActuator;
import it.uniroma2.edf.am.monitor.ApplicationMonitor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.EDFOptions;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.shaded.netty4.io.netty.handler.logging.LogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;


/**
 * Per-application controller.
 * May interact with decentralized OperatorManagers.
 */
public class ApplicationManager implements Runnable {

	protected JobGraph jobGraph;
	protected Dispatcher dispatcher;
	protected Configuration config;

	protected ApplicationMonitor appMonitor = null;
	protected GlobalActuator globalActuator;

	protected double ir;
	protected double oldir;
	protected Map<String, Integer> request = new HashMap<>();

	private BufferedWriter avglatFileWriter = null;
	private BufferedWriter proctimeFileWriter = null;
	private BufferedWriter latFileWriter = null;


	private int amInterval;
	private int roundsBetweenPlanning;

	protected static final Logger LOG = LoggerFactory.getLogger(ApplicationManager.class);

	public ApplicationManager (JobGraph jobGraph,
                               Configuration config,
                               Dispatcher dispatcher) {
		this.config = config;
		this.jobGraph = jobGraph;
		this.dispatcher = dispatcher;

		//String statsFilename = config.getString(EDFOptions.EDF_AM_STATS_FILENAME);
		String avglatFilename = "";
		String proctimeFileName = "proctimeout.txt";
		String latFileName = "onlylatout.txt";
		LOG.info("Writing stats to: {}", avglatFilename);
		if (!avglatFilename.isEmpty()) {
			try {
				avglatFileWriter = new BufferedWriter(new FileWriter(new File(avglatFilename), true));
				proctimeFileWriter = new BufferedWriter(new FileWriter(new File(proctimeFileName), true));
				latFileWriter = new BufferedWriter(new FileWriter(new File(latFileName), true));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		this.amInterval = config.getInteger(EDFOptions.AM_INTERVAL_SECS);
		this.roundsBetweenPlanning = config.getInteger(EDFOptions.AM_ROUNDS_BEFORE_PLANNING);

		this.globalActuator = new GlobalActuator();

	}

	protected void initialize()
	{
		LOG.info("AM: INITIALIZED!(modified)");
		appMonitor = new ApplicationMonitor(jobGraph, config);
	}

	@Override
	public void run() {
		initialize();

		int round = 0;

		while (true) {
			try {
				//Thread.sleep(amInterval*1000);
				Thread.sleep(3000);
			} catch (InterruptedException e) {}

			CompletableFuture<JobStatus> jobStatusFuture = dispatcher.requestJobStatus(jobGraph.getJobID(), Time.seconds(3));
			try {
				LOG.info("AM: GETTING JOB STATUS!");
				JobStatus jobStatus = jobStatusFuture.get();
				if (jobStatus == JobStatus.FAILED || jobStatus == JobStatus.CANCELED || jobStatus == JobStatus.CANCELLING) {
					LOG.info("AM: Closing.");
					close();
					return;
				}
				else if(jobStatus == JobStatus.FINISHED) {
						LOG.info("AM: JOB FINISHED!.");
				}
				else if (jobStatus != JobStatus.RUNNING) {
					LOG.warn("AM: Job NOT running...AM inhibited");
					continue;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}

            //round = (round + 1) % roundsBetweenPlanning;
			round = (round + 1);
			LOG.info("AM: Round " + round);
			monitor();

			if ((round % 2) == 0) {
				analyze();
				plan(round);
				execute();
			}


			if (avglatFileWriter != null) {
				try {
					avglatFileWriter.flush();
					proctimeFileWriter.flush();
					latFileWriter.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	protected void close()
	{
		LOG.info("AM: closed");
		//appMonitor.close();
	}

	protected void monitor() {
		LOG.info("Numero di vertici: "+jobGraph.getNumberOfVertices());
		LOG.info("Lista di vertici: "+jobGraph.getVertices());
		//jobGraph.getVertices().forEach(jobVertex -> LOG.info("operatorids " + jobVertex.getOperatorIDs()));
		//jobGraph.getVertices().forEach(jobVertex -> LOG.info("vertexname " + jobVertex.getName()));
		//jobGraph.getVertices().forEach(jobVertex -> LOG.info("vertexid " + jobVertex.getID()));
		//jobGraph.getVertices().forEach(jobVertex -> LOG.info("vertextostring " + jobVertex.toString()));
		JobGraphUtils.listUpstreamOperators(jobGraph, jobGraph.getVerticesAsArray()[1]).forEach(jobVertex -> LOG.info("upstreamtostirng "+jobVertex.toString()));
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
			EDFLogger.log("EDF: Input Rate: " + ir, LogLevel.INFO, ApplicationManager.class);
			double operatorIr = appMonitor.getOperatorInputRate(vertex.getName());
			double appIr = appMonitor.getApplicationInputRate();
			double operatorLatency = appMonitor.getAvgLatencyUpToOperator(vertex);
			double avgOperatorLatency = appMonitor.getAvgOperatorLatency(vertex);
			double processingTime = appMonitor.getAvgOperatorProcessingTime(vertex.getName());
			EDFLogger.log("EDF: operator Input Rate: " + operatorIr, LogLevel.INFO, ApplicationManager.class);
			EDFLogger.log("EDF: application Input Rate: " + appIr, LogLevel.INFO, ApplicationManager.class);
			EDFLogger.log("EDF: operatorLatency: " + operatorLatency, LogLevel.INFO, ApplicationManager.class);
			EDFLogger.log("EDF: avgOperatorLatency: " + avgOperatorLatency, LogLevel.INFO, ApplicationManager.class);
			EDFLogger.log("EDF: processingTime: " + processingTime, LogLevel.INFO, ApplicationManager.class);
			EDFLogger.log("EDF: avgLatency + processingTime: " + (processingTime+avgOperatorLatency), LogLevel.INFO, ApplicationManager.class);

			//Latencies print for experimentation
			if (avglatFileWriter != null) {
				try {
					avglatFileWriter.write(String.valueOf(processingTime + avgOperatorLatency));
					avglatFileWriter.newLine();
					proctimeFileWriter.write(String.valueOf(processingTime));
					proctimeFileWriter.newLine();
					latFileWriter.write(String.valueOf(avgOperatorLatency));
					latFileWriter.newLine();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}


		}
		this.oldir = this.ir;
		this.ir = ir;
	}

	protected void analyze() {
		LOG.info("ANALYZE - parallelism: " +jobGraph.getVerticesAsArray()[1].getParallelism());
		//LOG.info("ANALYZE - parallelism: " +jobGraph.getVerticesAsArray()[2].getParallelism());
	}

	protected void plan(int round) {
		if(round == 160){
			try{
			avglatFileWriter.write("SCALING");
			avglatFileWriter.newLine();
			proctimeFileWriter.write("SCALING");
			proctimeFileWriter.newLine();
			latFileWriter.write("SCALING");
			latFileWriter.newLine();
			} catch (IOException e){
			e.printStackTrace();
			}
			/*
			ArrayList<Integer> resTypes = this.jobGraph.getTaskResTypes().get((jobGraph.getVerticesAsArray()[1].getID()));
			resTypes.clear();
			resTypes.add(0);
			this.jobGraph.getTaskResTypes().put(jobGraph.getVerticesAsArray()[1].getID(),resTypes);
			this.request.put(jobGraph.getVerticesAsArray()[1].getID().toString(), 1);
			*/


			 for (ArrayList<Integer> list: this.jobGraph.getTaskResTypes().values()){
			 	list.clear();
			 	list.add(0);
			 }
			this.request.put(jobGraph.getVerticesAsArray()[1].getID().toString(), 1);

			/*
			ArrayList<Integer> resTypes = this.jobGraph.getTaskResTypes().get((jobGraph.getVerticesSortedTopologicallyFromSources().get(1).getID()));
			resTypes.clear();
			resTypes.add(0);
			this.request.put(jobGraph.getVerticesSortedTopologicallyFromSources().get(1).getID().toString(), 1);

			 */
		}
		/*
		else if(round == 8){
			ArrayList<Integer> resTypes = this.jobGraph.getTaskResTypes().get((jobGraph.getVerticesAsArray()[2].getID()));
			resTypes.add(1);
			this.jobGraph.getTaskResTypes().put(jobGraph.getVerticesAsArray()[2].getID(),resTypes);
			this.request.put(jobGraph.getVerticesAsArray()[2].getID().toString(), 3);
		}

		*/
		//else if(round == 12){
		//	this.request.put(jobGraph.getVerticesAsArray()[1].getID().toString(), 2);
		//}
		else{
			//this.request.remove(jobGraph.getVerticesAsArray()[1].getID().toString());
			this.request.clear();
		}
		/*
		if(this.ir >= 1 && this.oldir < 1) {
			this.request.put(jobGraph.getVerticesAsArray()[1].getID().toString(), 2);
		}
		else if(this.ir >= 1.6 && this.oldir < 1.6) {
			this.request.put(jobGraph.getVerticesAsArray()[2].getID().toString(), 2);
		}
		else if(this.ir <= 0.5 && this.oldir > 0.5) {
			this.request.put(jobGraph.getVerticesAsArray()[1].getID().toString(), 1);
		}
		else{
			//this.request.remove(jobGraph.getVerticesAsArray()[1].getID().toString());
			this.request.clear();
		}

		 */
	}

	protected void execute() {
		if (this.request.size() > 0) {
			LOG.info("RESCALING OPERATOR");
			globalActuator.rescale(this.dispatcher, this.jobGraph, this.request);
			LOG.info("RESCALED?");
			/*
			try {
				Thread.sleep(50000);
			} catch (InterruptedException e) {}

			 */
		}
	}


}
