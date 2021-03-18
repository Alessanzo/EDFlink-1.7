package it.uniroma2.edf;

import it.uniroma2.edf.utils.EDFLogger;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.slotpool.SchedulingStrategy;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.shaded.netty4.io.netty.handler.logging.LogLevel;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class HEDFSchedulingStrategy implements SchedulingStrategy {

	private static final HEDFSchedulingStrategy INSTANCE = new HEDFSchedulingStrategy();

	HEDFSchedulingStrategy(){}

	@Override
	public <IN, OUT> OUT findMatchWithLocality(
		@Nonnull SlotProfile slotProfile,
		@Nonnull Supplier<Stream<IN>> candidates,
		@Nonnull Function<IN, SlotInfo> contextExtractor,
		@Nonnull Predicate<IN> additionalRequirementsFilter,
		@Nonnull BiFunction<IN, Locality, OUT> resultProducer) {

		if (slotProfile.isStrictSchedReq())
			return doFindMatchStrict(
				slotProfile,
				candidates.get(),
				contextExtractor,
				additionalRequirementsFilter,
				resultProducer);

		else
			return doFindMatchRelaxed(
				slotProfile,
				candidates.get(),
				contextExtractor,
				additionalRequirementsFilter,
				resultProducer);
	}

	//STRICT approach for choosing the Slot: a Slot is returned if and only if its resType matches strictly with the one requested
	protected  <IN, OUT> OUT doFindMatchStrict(
		@Nonnull SlotProfile slotProfile,
		@Nonnull Stream<IN> candidates,
		@Nonnull Function<IN, SlotInfo> contextExtractor,
		@Nonnull Predicate<IN> additionalRequirementsFilter,
		@Nonnull BiFunction<IN, Locality, OUT> resultProducer) {

		EDFLogger.log("HEDF: Slot Allocation by SlotPool. Strategy: STRICT", LogLevel.INFO, HEDFSchedulingStrategy.class);

		Iterator<IN> iterator = candidates.iterator();

		//check all candidates and return inly if resType matches
		while (iterator.hasNext()) {
			IN candidate = iterator.next();
			if (additionalRequirementsFilter.test(candidate)) {
				SlotInfo slotContext = contextExtractor.apply(candidate);
				if (slotContext.getTaskManagerLocation().getResType() == slotProfile.getResourceProfile().getResourceType()) {
					EDFLogger.log("HEDF: Slot resType exact Matching in SlotPool", LogLevel.INFO, HEDFSchedulingStrategy.class);
					return resultProducer.apply(candidate, Locality.LOCAL);
				}
			}
		}

		EDFLogger.log("HEDF: No exact Matching Slot found in SlotPool", LogLevel.INFO, HEDFSchedulingStrategy.class);
		return  null;

	}

	//RELAXED approach for choosing the Slot: score based priority where exact resType matching has the absolute priority
	//then bigger resType than requested, then other Slots that best fit Locality requirements
	protected  <IN, OUT> OUT doFindMatchRelaxed(
		@Nonnull SlotProfile slotProfile,
		@Nonnull Stream<IN> candidates,
		@Nonnull Function<IN, SlotInfo> contextExtractor,
		@Nonnull Predicate<IN> additionalRequirementsFilter,
		@Nonnull BiFunction<IN, Locality, OUT> resultProducer) {

		EDFLogger.log("HEDF: Slot Allocation by SlotPool. Strategy: RELAXED", LogLevel.INFO, HEDFSchedulingStrategy.class);
		Iterator<IN> iterator = candidates.iterator();

		IN bestCandidate = null;
		int bestCandidateScore = Integer.MIN_VALUE;
		int currentCandidateScore = 0;

		while (iterator.hasNext()) {
			IN candidate = iterator.next();
			if (additionalRequirementsFilter.test(candidate)) {
				SlotInfo slotContext = contextExtractor.apply(candidate);

				if(slotContext.getTaskManagerLocation().getResType() >= slotProfile.getResourceProfile().getResourceType()){
					if(slotContext.getTaskManagerLocation().getResType() == slotProfile.getResourceProfile().getResourceType()) {
						currentCandidateScore = 100; //highest score to be topped just by other exactly matching Slots
					}
					else currentCandidateScore = 50;//second highest score
				}
				for (TaskManagerLocation location: slotProfile.getPreferredLocations()) {
					if (slotContext.getTaskManagerLocation().getResourceID() == location.getResourceID())
						currentCandidateScore++;//one more poit for every LocationPreference met
				}
				if (currentCandidateScore > bestCandidateScore) {
					bestCandidate = candidate;
					bestCandidateScore = currentCandidateScore;
				}
			}
		}

		// at the end of the iteration, we return the candidate with best possible locality or null.
		if (bestCandidate != null) {
			if (currentCandidateScore >= 100) EDFLogger.log("HEDF: Slot resType exact Matching in SlotPool", LogLevel.INFO, HEDFSchedulingStrategy.class);
			else if (currentCandidateScore >= 50) EDFLogger.log("HEDF: Slot chosen has bigger resType in SlotPool", LogLevel.INFO, HEDFSchedulingStrategy.class);
			else EDFLogger.log("HEDF: chosen Slot resType is not matching nor bigger in SlotPool", LogLevel.INFO, HEDFSchedulingStrategy.class);
			return resultProducer.apply(bestCandidate, Locality.LOCAL);
		} else {
			return null;
		}
	}

	public static HEDFSchedulingStrategy getInstance() {
		return INSTANCE;
	}
}
