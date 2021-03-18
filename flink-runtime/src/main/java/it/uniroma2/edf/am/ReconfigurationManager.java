package it.uniroma2.edf.am;

import it.uniroma2.dspsim.dsp.Operator;
import it.uniroma2.dspsim.dsp.Reconfiguration;
import it.uniroma2.dspsim.dsp.edf.om.OperatorManager;
import it.uniroma2.dspsim.dsp.edf.om.request.OMRequest;
import it.uniroma2.dspsim.infrastructure.NodeType;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/*AM component that checks requests*/
public class ReconfigurationManager {

	//requests are accepted
	public Map<Operator, Reconfiguration> acceptRequests(Map<OperatorManager, OMRequest> omRequestMap) {
		Map<Operator, Reconfiguration> reconfigurations = new HashMap<>(omRequestMap.size());

		for (OperatorManager om : omRequestMap.keySet()) {
			OMRequest req = omRequestMap.get(om);
			Reconfiguration rcf = req.getRequestedReconfiguration();
			if (rcf.isReconfiguration()) {
				reconfigurations.put(om.getOperator(), rcf);
			}
		}

		return reconfigurations;
	}

	//reconfiguration requests are adapted to the request map and resTypes list in JobGraph used in scheduling and scaling
	public void fillDesiredSchedulingReconf(Map<Operator, Reconfiguration> reconfigurations, JobGraph jobGraph, Map<String, Integer> request,
											Map<String, JobVertexID> perOperatorNameID){
		request.clear();
		for (Map.Entry<Operator, Reconfiguration> reconf: reconfigurations.entrySet()){
			Operator opToReconf = reconf.getKey();
			Reconfiguration opReconf = reconf.getValue();
			JobVertexID operatorID = perOperatorNameID.get(opToReconf.getName());
			int newParallelism = jobGraph.getTaskResTypes().get(operatorID).size();
			int resTypeToModify;
			if (opReconf.getInstancesToAdd() != null) {
				for (NodeType nodeTypeToModify: opReconf.getInstancesToAdd()){
					resTypeToModify = nodeTypeToModify.getIndex(); //get resType desired for replica to add
					jobGraph.getTaskResTypes().get(operatorID).add(resTypeToModify); //specify new resType in the list used at scheduling time
					newParallelism ++;
				}
				request.put(operatorID.toString(), newParallelism); //change number of replicas to scale
			}
			else if (reconf.getValue().getInstancesToRemove() != null) {
				for (NodeType nodeTypeToModify: opReconf.getInstancesToRemove()){
					resTypeToModify = nodeTypeToModify.getIndex(); //get resType of replica to remove
					jobGraph.getTaskResTypes().get(operatorID).remove(Integer.valueOf(resTypeToModify)); //remove replica for next scheduling
					newParallelism --;
				}
				request.put(operatorID.toString(), newParallelism); //change number of replicas to scale
			}
		}
	}

}
