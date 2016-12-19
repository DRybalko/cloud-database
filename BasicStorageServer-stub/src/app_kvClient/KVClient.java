package app_kvClient;

import java.io.IOException;

import org.apache.log4j.Level;

import logger.LogSetup;

/**
 * Main class for the client. 
 */
public class KVClient {

	private static final String LOG_DIRECTORY = "logs/client/client.log";
	private static final Level INITIALIZE_LOG_LEVEL = Level.ALL;
	
	public static void main(String[] args) {
		initializeLogging();
		CommandLineProcessor.readAndProcessInput();
	}
	
	private static void initializeLogging() {
		try {
			new LogSetup(LOG_DIRECTORY, INITIALIZE_LOG_LEVEL);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
