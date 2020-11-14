package it.uniroma2.edf.threads;

import it.uniroma2.edf.BashUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Manages operator threads.
 * TODO: This component should be moved in the TaskManager.
 * At the moment it only works for a local running TM.
 *
 * Requirements:
 * - jstack
 * - taskset
 * - awk, grep, ps, tr
 */
public class ThreadsManager {

	private Map<String, Integer> op2tid;

	private static Logger log = LoggerFactory.getLogger(ThreadsManager.class);

	public ThreadsManager (JobGraph jobGraph) {
		this.op2tid = createOperatorTidMapping(jobGraph);
	}

	private Map<String, Integer> createOperatorTidMapping(JobGraph jobGraph) {
		final int taskManagerTid = getLocalTaskmanagerPID();
		log.info("TM PID = {}", taskManagerTid);

		HashMap<String, Integer> mapping = new HashMap<>(jobGraph.getNumberOfVertices());

		for (JobVertex jv : jobGraph.getVertices()) {
			int tid = -1;
			final int MAX_RETRIES = 3;
			int retry_counter = 1;
			while (tid < 0 && retry_counter <= MAX_RETRIES) {
				tid = retrieveOperatorTID(jv.getName());

				if (tid < 0) {
					++retry_counter;
					try {
						Thread.sleep(1000);
						log.warn("Retrying to retrieve operator TID...");
					} catch (InterruptedException e) {
					}
				}
			}

			if (tid > 0) {
				log.info("TID: {} -> {}", jv.getName(), tid);
				mapping.put(jv.getName(), tid);
			} else {
				log.warn("Could not retrieve the TID for {}", jv.getName());
			}
		}

		return mapping;
	}

	public void pinOperatorOnCores() {
		int nCores = Runtime.getRuntime().availableProcessors();
		int coreId = 0;

		final String cmdFmt = "taskset -p -c %d %d";
		for (Integer tid : op2tid.values()) {
			log.info("Pinning {} on core {}", tid, coreId);
			BashUtils.exec(String.format(cmdFmt, coreId, tid));

			coreId = (coreId + 1) % nCores;
		}
	}


	public int getLocalTaskmanagerPID()
	{
		final String cmd = "ps aux | grep taskexecutor | grep flink | grep java | head -n 1 | awk '{ print $2 }'";
		String output = BashUtils.exec(cmd);
		if (output != null) {
			try {
				return Integer.parseInt(output);
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
		}

		return -1;
	}

	private Integer retrieveOperatorTID (String opNameToGrep)
	{
		final int pid = getLocalTaskmanagerPID();
		final String cmd = String.format("jstack %d | grep \"\\\"%s\" | egrep -o 'nid=0x\\w+' | sed 's/nid=0x//'", pid, opNameToGrep);
		String output = BashUtils.exec(cmd);
		if (output != null && !output.isEmpty()) {
			try {
				return Integer.parseInt(output, 16);
			} catch (NumberFormatException e) {
				log.warn("Failed to retrieve TID for operator: {}", opNameToGrep);
			}
		}

		return -1;
	}

	public Integer getOperatorTID (String opNameToGrep)
	{
		return op2tid.get(opNameToGrep);
	}

}
