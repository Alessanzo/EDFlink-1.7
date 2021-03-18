package it.uniroma2.edf.om;

import it.uniroma2.dspsim.Configuration;
import it.uniroma2.dspsim.dsp.Operator;
import it.uniroma2.dspsim.dsp.Reconfiguration;
import it.uniroma2.dspsim.dsp.edf.om.OMMonitoringInfo;
import it.uniroma2.dspsim.dsp.edf.om.ThresholdBasedOM;
import it.uniroma2.dspsim.dsp.edf.om.request.BasicOMRequest;
import it.uniroma2.dspsim.dsp.edf.om.request.OMRequest;
import it.uniroma2.dspsim.dsp.edf.om.threshold.MaxSpeedupThresholdPolicy;
import it.uniroma2.dspsim.dsp.edf.om.threshold.MinCostThresholdPolicy;
import it.uniroma2.dspsim.dsp.edf.om.threshold.RandomSelectionThresholdPolicy;
import it.uniroma2.dspsim.dsp.edf.om.threshold.ThresholdPolicy;
/*new Threshold Based extension that gives the possibility of choosing among 3 threshold policies configurable from
* config.policies, and overrides reconfiguration method to apply the threshold policy selected */
public class HEDFlinkThresholdBasedOM extends ThresholdBasedOM {

	public static final String THRESHOLD_POLICY = "edf.om.threshold.policy";

	private final ThresholdPolicy thresholdPolicy = selectThresholdPolicy();
	private final double scaleOutThreshold = Configuration.getInstance().getDouble("edf.om.threshold", 0.7D);

	public HEDFlinkThresholdBasedOM(Operator operator) {
		super(operator);
	}

	@Override
	public OMRequest pickReconfigurationRequest(OMMonitoringInfo monitoringInfo) {
		double u = monitoringInfo.getCpuUtilization();
		double p = (double)this.operator.getInstances().size();
		Reconfiguration rcf = this.thresholdPolicy.applyThresholdPolicy(u, p, this.operator, this.scaleOutThreshold);
		return new BasicOMRequest(rcf);
	}


	public static ThresholdPolicy selectThresholdPolicy(){
		String thPolicyType = Configuration.getInstance().getString(THRESHOLD_POLICY, "min-cost");
		switch (thPolicyType) {
			case "min-cost":
				return new MinCostThresholdPolicy();
			case "max-speedup":
				return new MaxSpeedupThresholdPolicy();
			case "random-selection":
				return new RandomSelectionThresholdPolicy();
			default:
				throw new IllegalArgumentException("Not valid operator manager type " + thPolicyType);
		}
	}
}
