package app_kvEcs.client;

import java.io.IOException;

import logger.LogSetup;

import org.apache.log4j.Level;

/*
 * Main class for ECS. Execution of the ECS starts here.
 */
public class ECSClient {

	private static final String LOG_DIRECTORY = "logs/ecs/ecs.log";
	private static final Level INITIALIZE_LOG_LEVEL = Level.ALL;
	
	public static void main(String[] args) {
		initializeLogging();
		EcsCommandLineProcessor.readAndProcessInput();
	}
	
	private static void initializeLogging() {
		try {
			new LogSetup(LOG_DIRECTORY, INITIALIZE_LOG_LEVEL);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
