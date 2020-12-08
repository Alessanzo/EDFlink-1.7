package it.uniroma2.edf.metrics;

import it.uniroma2.dspsim.infrastructure.ComputingInfrastructure;
import it.uniroma2.dspsim.stats.Statistics;
import it.uniroma2.dspsim.stats.metrics.CountMetric;
import it.uniroma2.dspsim.stats.metrics.Metric;
import it.uniroma2.dspsim.stats.metrics.RealValuedMetric;

public class EDFlinkStatistics extends Statistics {

	final String STAT_LATENCY_VIOLATIONS = "Violations";
	final String STAT_RECONFIGURATIONS = "Reconfigurations";
	final String STAT_RESOURCES_COST = "ResourcesCost";
	final String STAT_APPLICATION_COST_AVG = "AvgCost";
	final String STAT_DESIRED_OP_RECONFIGURATIONS = "DesOpReconf";
	final String STAT_DESIRED_RECONFIGURATIONS = "DesReconf";

	public EDFlinkStatistics(){
		super();
		registerMetrics();
	}

	public void registerMetrics(){

		registerMetric(new CountMetric(STAT_LATENCY_VIOLATIONS));
		registerMetric(new RealValuedMetric(STAT_APPLICATION_COST_AVG, true, true));
		registerMetric(new CountMetric(STAT_RECONFIGURATIONS));
		registerMetric(new RealValuedMetric(STAT_RESOURCES_COST));
		registerMetric(new CountMetric(STAT_DESIRED_OP_RECONFIGURATIONS));
		registerMetric(new CountMetric(STAT_DESIRED_RECONFIGURATIONS));
		/*
		this.metricDeployedInstances = new RealValuedMetric[ComputingInfrastructure.getInfrastructure().getNodeTypes().length];
		for (int i = 0; i < ComputingInfrastructure.getInfrastructure().getNodeTypes().length; i++) {
			this.metricDeployedInstances[i]	 = new RealValuedMetric("InstancesType" + i);
			registerMetric(this.metricDeployedInstances[i]);
		}

		 */
		for (int i = 0; i < ComputingInfrastructure.getInfrastructure().getNodeTypes().length; i++) {
			registerMetric(new RealValuedMetric("InstancesType" + i));
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

	public void updateDeployedInstances(){

	}
}
