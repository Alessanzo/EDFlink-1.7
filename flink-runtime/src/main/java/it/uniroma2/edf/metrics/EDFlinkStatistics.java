package it.uniroma2.edf.metrics;

import it.uniroma2.dspsim.Configuration;
import it.uniroma2.dspsim.ConfigurationKeys;
import it.uniroma2.dspsim.infrastructure.ComputingInfrastructure;
import it.uniroma2.dspsim.stats.Statistics;
import it.uniroma2.dspsim.stats.metrics.CountMetric;
import it.uniroma2.dspsim.stats.metrics.Metric;
import it.uniroma2.dspsim.stats.metrics.RealValuedMetric;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

public class EDFlinkStatistics extends Statistics {

	final String STAT_LATENCY_VIOLATIONS = "Violations";
	final String STAT_RECONFIGURATIONS = "Reconfigurations";
	final String STAT_RESOURCES_COST = "ResourcesCost";
	final String STAT_APPLICATION_COST_AVG = "AvgCost";
	final String STAT_DESIRED_OP_RECONFIGURATIONS = "DesOpReconf";
	final String STAT_NEWPLACEMENT_OP_MISCONF = "NewPlacementMisconf";
	final String STAT_OP_MISCONF = "ReplacementMisconf";
	final String STAT_DESIRED_RECONFIGURATIONS = "DesReconf";
	final String STAT_INSTANCES_TYPES = "InstanceType";

	final File statsOutput;

	public EDFlinkStatistics(){
		super();
		registerMetrics();

		statsOutput = new File(String.format("%s/final_stats",
			Configuration.getInstance().getString(ConfigurationKeys.OUTPUT_BASE_PATH_KEY, "")));
		if (!statsOutput.getParentFile().exists()) {
			statsOutput.getParentFile().mkdirs();
		}
	}

	public void dumpStats() {
		try {
			FileOutputStream fos = new FileOutputStream(statsOutput);
			dumpAll(fos);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void registerMetrics(){

		registerMetric(new CountMetric(STAT_LATENCY_VIOLATIONS));
		registerMetric(new RealValuedMetric(STAT_APPLICATION_COST_AVG));
		registerMetric(new CountMetric(STAT_RECONFIGURATIONS));
		registerMetric(new RealValuedMetric(STAT_RESOURCES_COST));
		registerMetric(new CountMetric(STAT_DESIRED_OP_RECONFIGURATIONS));
		registerMetric(new CountMetric(STAT_NEWPLACEMENT_OP_MISCONF));
		registerMetric(new CountMetric(STAT_OP_MISCONF));
		registerMetric(new CountMetric(STAT_DESIRED_RECONFIGURATIONS));
		for (int i = 0; i < ComputingInfrastructure.getInfrastructure().getNodeTypes().length; i++) {
			registerMetric(new RealValuedMetric(STAT_INSTANCES_TYPES + i));
		}
	}

	public void updateViolations(int value){
		getMetric(STAT_LATENCY_VIOLATIONS).update(value);
	}

	public void updateReconfigurations(int value){
		getMetric(STAT_RECONFIGURATIONS).update(value);
	}

	public void updateResCost(double value){
		getMetric(STAT_RESOURCES_COST).update(value);
	}

	public void updateAvgCost(double value){
		getMetric(STAT_APPLICATION_COST_AVG).update(value);
	}

	public void updateDesOpReconf(int value) {getMetric(STAT_DESIRED_OP_RECONFIGURATIONS).update(value);}

	public void updateDesReconf(int value) {getMetric(STAT_DESIRED_RECONFIGURATIONS).update(value);}

	public void updateNewPlacementMisconf(int value) {getMetric(STAT_NEWPLACEMENT_OP_MISCONF).update(value);}

	public void updateOpMisconf(int value) {getMetric(STAT_OP_MISCONF).update(value);}

	public void updateDeployedInstances(int nodeTypeIndex, int value){
		getMetric(STAT_INSTANCES_TYPES+nodeTypeIndex).update(value);}
}
