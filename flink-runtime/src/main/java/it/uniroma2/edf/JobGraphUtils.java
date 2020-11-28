package it.uniroma2.edf;

import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class JobGraphUtils {

	private static Logger log = LoggerFactory.getLogger(JobGraphUtils.class);

	private JobGraphUtils() {}


	static public List<JobVertex> listOperators (final JobGraph jobGraph, boolean ignoreSources, boolean ignoreSinks)
	{
		List<JobVertex> operators = new ArrayList<>();

		for (JobVertex v : jobGraph.getVertices()) {
			if (ignoreSources && v.isInputVertex())
				continue;
			if (ignoreSinks && v.isOutputVertex())
				continue;
			operators.add(v);
		}

		return operators;
	}

	static public List<JobVertex> listSortedTopologicallyOperators (final JobGraph jobGraph, boolean ignoreSources, boolean ignoreSinks)
	{
		List<JobVertex> operators = new ArrayList<>();

		for (JobVertex v : jobGraph.getVerticesSortedTopologicallyFromSources()) {
			if (ignoreSources && v.isInputVertex())
				continue;
			if (ignoreSinks && v.isOutputVertex())
				continue;
			operators.add(v);
		}

		return operators;
	}

	static public List<JobVertex> listSources (final JobGraph jobGraph)
	{
		List<JobVertex> sources = new ArrayList<>();

		for (JobVertex v : jobGraph.getVertices()) {
			if (v.isInputVertex())
				sources.add(v);
		}

		return sources;
	}

	static public List<JobVertex> listSinks (final JobGraph jobGraph)
	{
		List<JobVertex> sinks = new ArrayList<>();

		for (JobVertex v : jobGraph.getVertices()) {
			if (v.isOutputVertex())
				sinks.add(v);
		}

		return sinks;
	}

	static public Set<JobVertex> listUpstreamOperators (final JobGraph jobGraph, final JobVertex operator)
	{
		Set<JobVertex> upstreamOperators = new HashSet<>();

		List<JobEdge> edges = operator.getInputs();
		for (JobEdge e : edges) {
			upstreamOperators.add(e.getSource().getProducer());
		}

		return upstreamOperators;
	}

	static public Map<JobVertex, Set<JobVertex>> buildDownstreamOperatorsMap (final JobGraph jobGraph)
	{
		Map<JobVertex, Set<JobVertex>> op2downstreamOperators = new HashMap<>();

		for (JobVertex down : jobGraph.getVertices()) {
			for (JobEdge e : down.getInputs()) {
				/* op -> down */
				JobVertex op = e.getSource().getProducer();
				if (!op2downstreamOperators.containsKey(op))
					op2downstreamOperators.put(op, new HashSet<>());
				op2downstreamOperators.get(op).add(down);
			}
		}

		return op2downstreamOperators;
	}

	static public List<List<JobVertex>> listSourceSinkPaths (final JobGraph jobGraph)
	{
		List<List<JobVertex>> paths = new ArrayList<>();
		for (JobVertex src : listSources(jobGraph)) {
			paths.addAll(listSourceSinkPaths(jobGraph, src));
		}

		return paths;
	}

	static public List<List<JobVertex>> listSourceSinkPaths (final JobGraph jobGraph, final JobVertex src)
	{
		List<List<JobVertex>> paths = new ArrayList<>();

		Map<JobVertex, Set<JobVertex>> op2downstream = buildDownstreamOperatorsMap(jobGraph);
		//log.info("Downstream map: {}", op2downstream);

		LinkedList<List<JobVertex>> tempQueue = new LinkedList<>();
		ArrayList<JobVertex> path = new ArrayList<>();

		path.add(src);
		tempQueue.addLast(path);

		while (!tempQueue.isEmpty()) {
			List<JobVertex> p = tempQueue.getFirst();
			tempQueue.removeFirst();

			JobVertex last = p.get(p.size()-1);
			if (last.isOutputVertex() || !op2downstream.containsKey(last)) {
				paths.add(p);
				continue;
			}

			for (JobVertex downOp : op2downstream.get(last)) {
                if (!p.contains(downOp)) {
                	ArrayList<JobVertex> newpath = new ArrayList<>();
                	newpath.addAll(p);
                	newpath.add(downOp);
                	tempQueue.addLast(newpath);
				}
			}
		}


		return paths;
	}
}
