package it.uniroma2.edf.threads;

import it.uniroma2.edf.BashUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.EDFOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CPUVerticalScaler {

	private ThreadsManager threadsManager;

	final private int cpuPeriodUs;
	private int[] cpuQuotaUs;
	static private boolean cgNeedsSudo;
	private boolean ignoreCpuLimits;

	private Set<String> createdGroups;
	private Map<String, CgroupCpuUsage> monitoredCpuUsage;

	static private Pattern cpuUsageParsingPattern =  Pattern.compile("cpuacct.usage:\\s*(\\d+)");
	static private Pattern cpuThrottledTimeParsingPattern = Pattern.compile("throttled_time\\s+(\\d+)");

	private static Logger log = LoggerFactory.getLogger(CPUVerticalScaler.class);

	public CPUVerticalScaler (Configuration conf, ThreadsManager threadsManager) {
		this.threadsManager = threadsManager;
		this.createdGroups = new HashSet<>();
		this.monitoredCpuUsage = new HashMap<>();

		cgNeedsSudo = conf.getBoolean(EDFOptions.CGROUP_UTILS_NEED_SUDO);

		this.ignoreCpuLimits = conf.getBoolean(EDFOptions.CGROUP_DISABLE_CPU_LIMITS);
		this.cpuPeriodUs = conf.getInteger(EDFOptions.CGROUP_CPU_PERIOD_US);
		final String cpuQuotas = conf.getString(EDFOptions.CGROUP_CPU_QUOTAS_US);
		final String[] splitQuotas = cpuQuotas.split(",");
		this.cpuQuotaUs = new int[splitQuotas.length];
		for (int i = 0; i<splitQuotas.length; i++) {
			if (ignoreCpuLimits)
				this.cpuQuotaUs[i] = -1;
			else
				this.cpuQuotaUs[i] = Integer.parseInt(splitQuotas[i]);
		}

		log.info("Cgroup params: period={}, quotas={}", cpuPeriodUs, cpuQuotaUs);
	}



	public void initialize (Map<String, Integer> operator2level) {
		for (String opName : operator2level.keySet()) {
			int tid = threadsManager.getOperatorTID(opName);

			final String cgName = getCgroupName(opName);
			try {
				createCgroup(cgName, tid);
				/* set default CPU level */
				setCpuLevel(opName, operator2level.get(opName));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	// TODO: we should only update monitored values if the method has not been called recently
	public double monitorOperatorCPUUtilization (String opName) {
		final String cgName = getCgroupName(opName);
		return monitorCgroupCPUUtilization(cgName);
	}

	private double monitorCgroupCPUUtilization (String cgName) {
		CgroupCpuUsage last = monitoredCpuUsage.get(cgName);
		CgroupCpuUsage current = getCgroupCpuUsage(cgName);
		if (current == null) {
			return -1.0;
		}

		long used = current.usage-last.usage;
		long throttled = current.throttled-last.throttled;
		long millis = current.timestamp-last.timestamp;
		double u = (double)(used+throttled)/(millis*1000.0*1000.0);

		monitoredCpuUsage.put(cgName, current);
		return u;
	}

	public void setCpuLevel (String opName, int level) throws IOException {
		final String cgName = getCgroupName(opName);

		if (level >= cpuQuotaUs.length) {
			log.error("Invalid CPU level ({}) requested for op {}", level, opName);
			return;
		}

		log.info("Setting level {} for op {} (cg={})", level, opName, cgName);
		updateCgroupCpu(cgName, cpuQuotaUs[level], cpuPeriodUs);
	}


	public int getCpuLevelsCount() {
		return cpuQuotaUs.length;
	}

	public void close() {
		for (String cgName : createdGroups) {
			deleteCgroup(cgName);
		}
	}

	/**
	 * Create a cgroup for the specified thread.
	 */
	private void createCgroup (String cgName, int tid) throws IOException {
		log.info("Creating group '{}' for tid={}", cgName, tid);

		final String createCmd = "cgcreate -g cpu,cpuacct:" + cgName;
		if (BashUtils.exec(createCmd, cgNeedsSudo) == null) {
			throw new IOException("Could not create the cgroup!");
		}

		final String classifyCmd = String.format("cgclassify -g cpu,cpuacct:%s %d", cgName, tid);
		if (BashUtils.exec(classifyCmd, cgNeedsSudo) == null) {
			throw new IOException("cgclassify failed for tid " + tid);
		}

		log.info("Created group '{}' for tid={}", cgName, tid);
		createdGroups.add(cgName);
		monitoredCpuUsage.put(cgName, getCgroupCpuUsage(cgName));
	}

	static private void updateCgroupCpu (String cgroup, int cpuQuota, int cpuPeriod) throws IOException {
		final String cmd1 = String.format("cgset -r cpu.cfs_period_us=\"%d\" %s", cpuPeriod, cgroup);
		if (BashUtils.exec(cmd1, cgNeedsSudo) == null) {
			throw new IOException("cgset failed for group " + cgroup);
		}

		final String cmd2 = String.format("cgset -r cpu.cfs_quota_us=\"%d\" %s", cpuQuota, cgroup);
		if (BashUtils.exec(cmd2, cgNeedsSudo) == null) {
			throw new IOException("cgset failed for group " + cgroup);
		}
	}

	static private void deleteCgroup (String cgName) {
		log.info("Deleting {}", cgName);

		final String cmd = "cgdelete -g cpu,cpuacct:" + cgName;
		if (BashUtils.exec(cmd, cgNeedsSudo) == null) {
			log.warn("Deletion of {} failed", cgName);
		}
	}

	static private CgroupCpuUsage getCgroupCpuUsage (String cgName) {
		final String cmd = String.format("cgget -n %s -r cpu.stat -r cpuacct.usage | grep -E 'cpuacct|throttled_time'| tr '\\n' ';'", cgName);

		final String output = BashUtils.exec(cmd, cgNeedsSudo);
		final long timestamp = System.currentTimeMillis();
		if (output == null) {
			log.warn("CPU usage monitoring of {} failed", cgName);
			return null;
		}

		long usage, throttledTime;
		try {
			Matcher usageMatcher = cpuUsageParsingPattern.matcher(output);
			if (usageMatcher.find()) {
				usage = Long.parseLong(usageMatcher.group(1));
			} else {
				log.warn("Usage not found!");
				return null;
			}

			usageMatcher = cpuThrottledTimeParsingPattern.matcher(output);
			if (usageMatcher.find()) {
				throttledTime = Long.parseLong(usageMatcher.group(1));
			} else {
				log.warn("Throttled time not found!");
				return null;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

		return new CgroupCpuUsage(timestamp, usage, throttledTime);
	}


	static private String getCgroupName (String operatorName)
	{
		return operatorName.replaceAll("[^a-zA-Z0-9]", "");
	}


	static private class CgroupCpuUsage {
		private long timestamp;
		private long usage;
		private long throttled;

		public CgroupCpuUsage(long timestamp, long usage, long throttled) {
			this.timestamp = timestamp;
			this.usage = usage;
			this.throttled = throttled;
		}

		public long getTimestamp() {
			return timestamp;
		}

		public long getUsage() {
			return usage;
		}

		public long getThrottled() {
			return throttled;
		}
	}
}
