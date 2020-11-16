package it.uniroma2.edf;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.slotpool.SchedulingStrategy;
import org.apache.flink.runtime.taskmanager.Task;
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

public class EDFSchedulingStrategy implements SchedulingStrategy {

	private static final EDFSchedulingStrategy INSTANCE = new EDFSchedulingStrategy();

	private static final BiFunction<Integer, Integer, Integer> LOCALITY_EVALUATION_FUNCTION = (localWeigh, hostLocalWeigh) -> localWeigh * 10 + hostLocalWeigh;

	EDFSchedulingStrategy(){}

	@Nullable
	@Override
	public <IN, OUT> OUT findMatchWithLocality(
		@Nonnull SlotProfile slotProfile,
		@Nonnull Supplier<Stream<IN>> candidates,
		@Nonnull Function<IN, SlotInfo> contextExtractor,
		@Nonnull Predicate<IN> additionalRequirementsFilter,
		@Nonnull BiFunction<IN, Locality, OUT> resultProducer) {

		return doFindMatchWithLocality(
			slotProfile,
			candidates.get(),
			contextExtractor,
			additionalRequirementsFilter,
			resultProducer);
	}

	@Nullable
	protected  <IN, OUT> OUT doFindMatchWithLocality(
		@Nonnull SlotProfile slotProfile,
		@Nonnull Stream<IN> candidates,
		@Nonnull Function<IN, SlotInfo> contextExtractor,
		@Nonnull Predicate<IN> additionalRequirementsFilter,
		@Nonnull BiFunction<IN, Locality, OUT> resultProducer) {
		Collection<TaskManagerLocation> locationPreferences = slotProfile.getPreferredLocations();

		EDFLogger.log("EDF: Scheduler in azione per schedulare un Task tra gli Slot dello SlotPool", LogLevel.INFO, EDFSchedulingStrategy.class);

		// if we have no location preferences, we can only filter by the additional requirements.
		if (locationPreferences.isEmpty()) {
			EDFLogger.log("EDF: Le LocationPreferences del Task sono vuote!", LogLevel.INFO, EDFSchedulingStrategy.class);
			return candidates
				.filter(additionalRequirementsFilter)
				.findFirst()
				.map((result) -> resultProducer.apply(result, Locality.UNCONSTRAINED))
				.orElse(null);
		}
		//EDFLogger.log("EDF: Le LocationPreferences del Task NON sono vuote!", LogLevel.INFO, EDFSchedulingStrategy.class);
		// we build up two indexes, one for resource id and one for host names of the preferred locations.
		final Map<ResourceID, Integer> preferredResourceIDs = new HashMap<>(locationPreferences.size());
		final Map<String, Integer> preferredFQHostNames = new HashMap<>(locationPreferences.size());

		for (TaskManagerLocation locationPreference : locationPreferences) {
			preferredResourceIDs.merge(locationPreference.getResourceID(), 1, Integer::sum);
			preferredFQHostNames.merge(locationPreference.getFQDNHostname(), 1, Integer::sum);
		}

		Iterator<IN> iterator = candidates.iterator();
		/*
		while (iterator.hasNext()) {
			IN candidate = iterator.next();
			SlotInfo slotContext = contextExtractor.apply(candidate);
			if (slotContext.getResourceType() == slotProfile.getResourceProfile().getResourceType()) {

			}
		}
		*/
		IN bestCandidate = null;
		int bestCandidateScore = Integer.MIN_VALUE;
		int currentCandidateScore = 0;
		int i = 0;
		while (iterator.hasNext()) {
			IN candidate = iterator.next();
			if (additionalRequirementsFilter.test(candidate)) {
				SlotInfo slotContext = contextExtractor.apply(candidate);
				//EDFLogger.log("Candidato numero " + i + ",con AllocationID: " + slotContext.getAllocationId(),
					//LogLevel.INFO, EDFSchedulingStrategy.class);
				i++;
				//EDFLogger.log("ResourceType dello Slot Candidato: "+ slotContext.getResourceType(), LogLevel.INFO, EDFSchedulingStrategy.class);
				//EDFLogger.log("ResourceType da ResourceProfile dello Slot Richiesto: "+ slotProfile.getResourceProfile().getResourceType(), LogLevel.INFO, EDFSchedulingStrategy.class);
				// this gets candidate is local-weigh

				/*
				if(slotContext.getResourceType() == slotProfile.getResourceProfile().getResourceType()) {
					EDFLogger.log("ResourceType matcha con richiesta!", LogLevel.INFO, EDFSchedulingStrategy.class);
					return resultProducer.apply(candidate, Locality.LOCAL);
				}
				*/


				if(slotContext.getResourceType() >= slotProfile.getResourceProfile().getResourceType()){
					if(slotContext.getResourceType() == slotProfile.getResourceProfile().getResourceType()) {
						currentCandidateScore = 100000;
					}
					else currentCandidateScore = 50000;
				}
				for (TaskManagerLocation location: slotProfile.getPreferredLocations()) {
					if (slotContext.getTaskManagerLocation().getResourceID() == location.getResourceID())
						currentCandidateScore++;
				}
				if (currentCandidateScore > bestCandidateScore) {
					bestCandidate = candidate;
					bestCandidateScore = currentCandidateScore;
				}




				//TEST RICERCA STATICA
				/*
				if (slotContext.getResourceType() == 1){
					EDFLogger.log("ResourceType matcha con richiesta 1!: "+ slotContext.getTaskManagerLocation().getResourceID() + " - " + slotContext.getAllocationId(), LogLevel.INFO, EDFSchedulingStrategy.class);
					return resultProducer.apply(candidate, Locality.LOCAL);
				}
				*/
			}
			//return resultProducer.apply(bestCandidate, Locality.LOCAL);
		}

		// at the end of the iteration, we return the candidate with best possible locality or null.
		if (bestCandidate != null) {
			if (currentCandidateScore >= 100000) EDFLogger.log("EDF: Lo Slot scelto dallo Scheduler matcha!", LogLevel.INFO, EDFSchedulingStrategy.class);
			else if (currentCandidateScore >= 50000) EDFLogger.log("EDF: Lo Slot scelto dallo Scheduler non matcha, ma ha un tipo maggiore", LogLevel.INFO, EDFSchedulingStrategy.class);
			else EDFLogger.log("EDF: Lo Slot scelto dallo Scheduler non matcha", LogLevel.INFO, EDFSchedulingStrategy.class);
			return resultProducer.apply(bestCandidate, Locality.LOCAL);
		} else {
			return null;
		}


		//return  null;
	}

	public static EDFSchedulingStrategy getInstance() {
		return INSTANCE;
	}
}
