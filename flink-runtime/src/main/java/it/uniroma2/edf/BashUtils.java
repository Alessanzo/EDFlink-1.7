package it.uniroma2.edf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class BashUtils {

	private final static Logger log = LoggerFactory.getLogger(BashUtils.class);

	private BashUtils() {}


	/**
	 * Execute a bash command. We can handle complex bash commands including
	 * multiple executions (; | && ||), quotes, expansions ($), escapes (\), e.g.:
	 *     "cd /abc/def; mv ghi 'older ghi '$(whoami)"
	 * @param command
	 */
	public static String exec (String command) {
		//log.info("Bash: {}", command);

		Runtime r = Runtime.getRuntime();
		// Use bash -c so we can handle things like multi commands separated by ; and
		// things like quotes, $, |, and \. My tests show that command comes as
		// one argument to bash, so we do not need to quote it to make it one thing.
		// Also, exec may object if it does not have an executable file as the first thing,
		// so having bash here makes it happy provided bash is installed and in path.
		String[] commands = {"bash", "-c", command};
		try {
			Process p = r.exec(commands);

			p.waitFor();
			BufferedReader b = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line = "";

			StringBuilder sb = new StringBuilder();

			while ((line = b.readLine()) != null) {
				sb.append(line);
			}

			b.close();
			return sb.toString();
		} catch (Exception e) {
			log.error("Command failed.");
			e.printStackTrace();
		}

		return null;
	}

	public static String exec (String command, boolean withSudo) {
		if (withSudo)
			return exec("sudo " + command);
		else
			return exec(command);
	}
}
