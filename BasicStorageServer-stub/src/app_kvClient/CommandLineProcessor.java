package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import logger.LogSetup;

import org.apache.log4j.*;

import common.messages.KVMessage;
import client.KVCommInterface;
import client.KVStore;

/** 
 * This class is needed to process the user input from the console. 
 * It reads the input, analyzes it and differentiates the command from the 
 * rest of the message.
 * This class gets the address and the port number and creates an Object of type
 * Communicator that is used to build the connection to the server.
 * It also provides the commands: connect, disconnect, send, logLevel, help and quit
 * from which the user can chose. 
 * 
 * @see Communicator
 */

public class CommandLineProcessor {

	private static String[] input;
	private static KVCommInterface kvStore;
	private static Logger logger = Logger.getRootLogger();
	private static boolean quit;

	private static final String LINE_START = "EchoClient> ";
	private static final String HELP_MESSAGE = "Welcome to EchoClient!\r\n\r\n"
			+ "Commands:\r\nconnect <address> <port> - tries to establish a TCP"
			+ "- connection to the server based on the given server address and "
			+ "the port\r\ndisconnect - tries to disconnect from the connected "
			+ "server\r\nsend <message> - sends a text message to the echo server "
			+ "according to the communication protocol\r\nlogLevel<level> - sets "
			+ "the logger to the specified log level\r\nhelp - information about "
			+ "all possible commands\r\nquit - tears down the active connection "
			+ "to the server and exits the program execution\n";

	public static void readAndProcessInput() {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		while (!quit) {
			readInputLine(br);
		}
	}
	
	private static void readInputLine(BufferedReader br) {
		System.out.print(LINE_START);
		try {
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
		case "connect": connect();
						break;
		case "disconnect": disconnect();
						break;
		case "put": put();
						break;
		case "get": get();
						break;
		case "logLevel": logLevel();
						break;
		case "help": help();
						break;
		case "quit": quit();
						break;
		default: errorMessage();
		}
	}

	private static void connect() {
		if (input.length >= 3) {
			kvStore = new KVStore(input[1], Integer.valueOf(input[2]));
			establishConnection();
		} else {
			errorMessage();
		}
	}
	
	private static void establishConnection() {
		try {
			kvStore.connect();
			System.out.println("Connection was established successfuly");
		} catch (Exception e) {
			System.out.println(LINE_START + "Error. Connection was not established successfuly");
			logger.info(e.getMessage());
		}
	}

	private static void disconnect() {
		kvStore.disconnect();
			System.out.println(LINE_START + "Connection terminated.");
	}

	private static void logLevel() {
		if (input.length >= 2) {
			String logLevel = input[1];
			logger.getRootLogger().setLevel(Level.toLevel(logLevel));
			System.out.println(LINE_START + "Current logging level: "
					+ logger.getLevel());
		} else {
			errorMessage();
		}
	};

	private static void put() {
		if (input.length >= 3) {
			try {
				StringBuilder stringBuilder = new StringBuilder();
				for (int i = 2; i < input.length; i++) {
					stringBuilder.append(input[i]);
				}
				KVMessage message = kvStore.put(input[1], stringBuilder.toString());
				System.out.println(message.toString());
			} catch (Exception e) {
				logger.info("Put failed. "+e.getMessage());
			}
		} else {
			errorMessage();
		}
	}
	
	private static void get() {
		if (input.length >= 2) {
			try {
				KVMessage message = kvStore.get(input[1]);
				System.out.println(message.toString());
			} catch (Exception e) {
				logger.info("Get failed. " + e.getMessage());
			}
		} else {
			errorMessage();
		}
	}

	private static void help() {
		System.out.println(LINE_START + HELP_MESSAGE);
	};

	private static void quit() {
		System.out.println(LINE_START + "Application exit. Bye ;)");
		quit = true;
	};

	private static void errorMessage() {
		System.out.println(LINE_START + "Error. Unknown command.");
		help();
	};

}
