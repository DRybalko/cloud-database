package app_kvEcs.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

import app_kvEcs.logic.ECSLogic;
import app_kvEcs.logic.Repository;

public class EcsCommandLineProcessor {

	private static String[] input;
	private static Logger logger = Logger.getRootLogger();
	private static boolean quit;
	private static ECSLogic ecsLogic;

	public static void readAndProcessInput() {
		Repository repository = new Repository("ecs.config");
		ecsLogic = new ECSLogic(repository);
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		while (!quit) {
			readInputLine(br);
		}
	}

	private static void readInputLine(BufferedReader br) {
		try {
			System.out.print("ECS> ");
			String inputLine = br.readLine();
			input = inputLine.split(" ");
			parseInput();
		} catch (IOException e) {
			logger.error("Error caused by wrong console input");
		}
	}

	private static void parseInput() {
		String command = input[0];
		switch (command) {
		case "initService": initService();
						break;
		case "start": ecsLogic.start();
						break;
		case "stop": ecsLogic.stop();
						break;
		case "shutDown": ecsLogic.shutDown();
						break;
		case "addNode": addNode();
						break;
		case "removeNode": ecsLogic.removeNode();
						break;
		case "quit": quit();
						break;
		default: errorMessage();
		}
	}
	
	private static void initService() {
		if (input.length >= 3) {
			ecsLogic.initService(Integer.parseInt(input[1]), Integer.parseInt(input[2]), input[3]);
		} else {
			errorMessage();
		}
	}
	
	private static void addNode() {
		if (input.length >= 2) {
			ecsLogic.addNode(Integer.parseInt(input[1]), input[2]);
		} else {
			errorMessage();
		}
	}
	
	private static void errorMessage() {
		System.out.println("Error. Unknown command.");	
	};

	private static void quit() {
		System.out.println("Application exit. Bye ;)");
		quit = true;
	};
}

