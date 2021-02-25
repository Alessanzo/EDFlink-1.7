package it.uniroma2.edf.utils;

import org.apache.flink.shaded.netty4.io.netty.handler.logging.LogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EDFLogger {

	public static synchronized void log(String text, LogLevel level, Class writeClass){

		Logger logger = LoggerFactory.getLogger(writeClass);
		switch (level){
			case INFO:
				logger.info(text);
				break;
				case DEBUG:
					logger.debug(text);
					break;

					case WARN:
						logger.warn(text);
						break;
						default:
							logger.info(text);
							break;
		}
	}
}
