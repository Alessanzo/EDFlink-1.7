package it.uniroma2.edf.om;

import it.uniroma2.dspsim.dsp.Operator;
import it.uniroma2.dspsim.dsp.edf.om.*;
import it.uniroma2.edf.om.HEDFlinkThresholdBasedOM;

/*New OperatorManagerFactory that adds the new HEDFlinkThresholdBasedOM with respect to old OMFactory from Simulator*/
public class HEDFlinkOperatorManagerFactory {


	public static OperatorManager createOperatorManager(OperatorManagerType operatorManagerType, Operator operator) throws IllegalArgumentException {
		switch(operatorManagerType) {
			case DO_NOTHING:
				return new DoNothingOM(operator);
			case THRESHOLD_BASED:
				return new HEDFlinkThresholdBasedOM(operator);
			case Q_LEARNING:
				return new QLearningOM(operator);
			case Q_LEARNING_PDS:
				return new QLearningPDSOM(operator);
			case FA_Q_LEARNING:
				return new FAQLearningOM(operator);
			case DEEP_Q_LEARNING:
				return new DeepQLearningOM(operator);
			case DEEP_V_LEARNING:
				return new DeepVLearningOM(operator);
			case VALUE_ITERATION:
				return new ValueIterationOM(operator);
			case MODEL_BASED:
				return new ModelBasedRLOM(operator);
			case VALUE_ITERATION_SPLITQ:
				return new ValueIterationSplitQOM(operator);
			case FA_TRAJECTORY_BASED_VALUE_ITERATION:
				return new FaTBValueIterationOM(operator);
			case DEEP_TRAJECTORY_BASED_VALUE_ITERATION:
				return new DeepTBValueIterationOM(operator);
			case FA_HYBRID:
				return new FAHybridRewardBasedOM(operator);
			case DEEP_HYBRID:
				return new DeepHybridRewardBasedOM(operator);
			default:
				throw new IllegalArgumentException("Not valid operator manager type: " + operatorManagerType.toString());
		}
	}
}
