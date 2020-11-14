package it.uniroma2.edf;

import org.apache.flink.util.Preconditions;

import java.util.concurrent.atomic.AtomicLong;

public class EDFUtils {

	static final String taskManagerPrefix = "taskmanager";
	private static final AtomicLong nextNameOffset = new AtomicLong(1L);


	public static String createRandomName() {

		long nameOffset;
		// obtain the next name offset by incrementing it atomically
		do {
			nameOffset = nextNameOffset.get();
		} while (!nextNameOffset.compareAndSet(nameOffset, nameOffset + 1L));

		return taskManagerPrefix + '_' + nameOffset;
	}
}
