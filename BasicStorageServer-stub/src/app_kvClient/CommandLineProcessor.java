package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDateTime;

import org.apache.log4j.*;

import common.logic.Value;
import common.messages.clientToServerMessage.KVMessage;
import common.messages.clientToServerMessage.KVMessage.KvStatusType;
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
 * @see KVCommInterface
 */
public class CommandLineProcessor {

	private static String[] input;
	private static KVCommInterface kvStore;
	private static Logger logger = Logger.getRootLogger();
	private static boolean quit;
	private static boolean loggedIn;
	private static PermissionController permissionController;

	private static final String LINE_START = "Client> ";
	private static final String HELP_MESSAGE = "Welcome to Key value storage client!\r\n\r\n"
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
		case "subscribe": subscribe();
						break;
		case "unsubscribe": unsubscribe();
						break;
		case "logout":  logOut();
						break;
		case "login":  	logIn();
						break;
		default: errorMessage();
		}
	}

	private static void connect() {
		kvStore = new KVStore();
		permissionController = new PermissionController();
		if (!loggedIn) logIn();
		try {
			kvStore.connect();
			System.out.println("Connection was established successfuly");
		} catch (Exception e) {
			System.out.println(LINE_START + "Error. Connection was not established successfuly");
			logger.info(e.getMessage());
		}
	}
	
	private static void logIn() {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		try {
			String role = "";
			int permission = -1;
			String username = "null";
			while (permission < 0 || username.equals("null")) {
				System.out.print(LINE_START + "sign in as: ");
				role = br.readLine();
				String[] stringTokens = role.split(" ");
				permission = permissionController.getPermissionLevel(stringTokens[0]); 
				if (permission < 0 || stringTokens.length < 2) System.out.println("Please sign in with valid user name");
				else username = stringTokens[1];
			}
			kvStore.setPermission(permission);
			kvStore.setUsername(username);
			loggedIn = true;
			System.out.println("You signed in as: " + role);
		} catch (IOException e1) {
			logger.error("Error caused by wrong console input");
		}
	}
	
	private static void logOut() {
		System.out.println(LINE_START + "Logged out.");
		loggedIn = false;
	}
	
	private static void disconnect() {
		kvStore.disconnect();
			System.out.println(LINE_START + "Connection terminated.");
	}

	private static void logLevel() {
		if (input.length >= 2) {
			String logLevel = input[1];
			Logger.getRootLogger().setLevel(Level.toLevel(logLevel));
			System.out.println(LINE_START + "Current logging level: "
					+ logger.getLevel());
		} else {
			errorMessage();
		}
	};

	private static void put() {
		if (!loggedIn) {
			System.out.println(LINE_START + "Please sign in");
			return;
		}
		if (input.length >= 3) {
			try {
				String putInput = readPutInput();
				LocalDateTime timestamp = LocalDateTime.now();
				Value value  = new Value(kvStore.getPermission(), kvStore.getUsername(), timestamp, putInput);			
				KVMessage message = kvStore.put(input[1], value);
				System.out.println(message.toString());
			} catch (Exception e) {
				logger.info("Put failed. "+e.getMessage());
			}
		} else errorMessage();
	}
	
	private static String readPutInput() {
		StringBuilder stringBuilder = new StringBuilder();
		for (int i = 2; i < input.length; i++) {
			stringBuilder.append(input[i]);
			stringBuilder.append(" ");
		}
		return stringBuilder.toString();
	}

	private static void get() {
		if (!loggedIn) {
			System.out.println(LINE_START + "Please sign in");
			return;
		}
		if (input.length == 2) {
			try {
				getKeyVersions();
			} catch (Exception e) {
				logger.info("Get failed.");
				System.out.println("Get failed.");
			}
		} else if (input.length == 3) {
			try {
				getValueForKeyAndVersion();
			} catch (Exception e) {
				logger.info("Get failed.");			
				System.out.println("Get failed.");
			}
		} else errorMessage();
	}
	
	private static void subscribe() {
		if (!loggedIn) {
			System.out.println(LINE_START + "Please sign in");
			return;
		}
		if (input.length >= 2) {
			try {
				KVMessage reply = kvStore.sendSubscriptionStatusMessage(input[1], KvStatusType.SUBSCRIBE);
				System.out.println(reply.toString());
			} catch (Exception e) {
				logger.info("Subscription failed");
			}
		} else errorMessage();
	}

	private static void unsubscribe() {
		if (!loggedIn) {
			System.out.println(LINE_START + "Please sign in");
			return;
		}
		if (input.length >= 2) {
			try {
				KVMessage reply = kvStore.sendSubscriptionStatusMessage(input[1], KvStatusType.UNSUBSCRIBE);
				System.out.println(reply.toString());
			} catch (Exception e) {
				logger.info("It was not possible to end subscribtion. Error occured.");
			}
		} else errorMessage();
	}
	
	private static void getValueForKeyAndVersion() throws Exception {
		int version = Integer.valueOf(input[2]);
		KVMessage message = kvStore.get(input[1], version);
		if (!message.getStatus().equals(KvStatusType.NO_PERMISSION)) printGetResult(message);
		else System.out.println("You have no permission to watch this data");
	}

	private static void getKeyVersions() throws Exception {
		KVMessage message = kvStore.getVersion(input[1]);
		if (message.getStatus().equals(KvStatusType.VERSION) && message.getVersion() > 0) {
			System.out.println("For this key there are " + message.getVersion() +" versions. Please specify which version you"
					+ " would like to get.");
		} else if (message.getStatus().equals(KvStatusType.NO_PERMISSION)) {
			System.out.println(message.toString());
		} else 	printGetResult(message);
	}

	private static void printGetResult(KVMessage message) {
		Value value = message.getValue();
		System.out.println("Result of get operation is: "+value.getValue());
		System.out.println("This document was created/updated on: "+value.getTimestamp().toString());
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
