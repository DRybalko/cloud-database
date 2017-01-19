package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.log4j.*;

import common.logic.Value;
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
 * @see KVCommInterface
 */

public class CommandLineProcessor {

	private static String[] input;
	private static KVCommInterface kvStore;
	private static Logger logger = Logger.getRootLogger();
	private static boolean quit;
	private static int permission;
	private static int vPermission;

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
	private static void logIn (){
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.print(LINE_START + "log in as: ");
		try {
			String role = br.readLine();
			switch(role){
			case "admin" : permission = 3;
			break;
			case "trainer" : permission = 2;
			break;
			case "user" : permission = 1;
			break;
			default : permission = 0;
			}
		} catch (IOException e1) {
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
		case "permissionchange" : permissionChange();
						break;
		default: errorMessage();
		}
	}

	private static void connect() {
		logIn();
		kvStore = new KVStore();
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
			Logger.getRootLogger().setLevel(Level.toLevel(logLevel));
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
					stringBuilder.append(" ");
				}
				System.out.println(LINE_START + "Please specify permission level: ");
				System.out.print(LINE_START);
				
				BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
				String role = br.readLine();
				
				switch(role){
				case "admin" : vPermission = 3;
					break;
				case "trainer" : vPermission = 2;
					break;
				case "user" : vPermission = 1;
					break;
				default : vPermission = 0;
				}
			
				Value value  = new Value(vPermission, 
						new SimpleDateFormat("yyyy/MM/dd_HH/mm/ss/SS").format(Calendar.getInstance().getTime()),
						stringBuilder.toString());
				
				KVMessage message = kvStore.put(input[1], value);
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
				System.out.println(permission);
				System.out.println(message.getValue().toString());
				System.out.println(message.getValue().getPermission());
				if(message.getValue().getPermission() < permission){
					System.out.println(message.toString());
				}else{
					System.out.println("You do not have permission to access this key.");
				}
			} catch (Exception e) {
				logger.info("Get failed. " + e.getMessage());
			}
		} else {
			errorMessage();
		}
	}
	
	private static void permissionChange(){
		if(permission == 3){
			try {
				KVMessage message = kvStore.get(input[1]);
				System.out.println(LINE_START + "Please enter new permission level: ");
				System.out.print(LINE_START);
				
				BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
				String role = br.readLine();
				int p = 0;
				
				switch(role){
				case "admin" : p = 3;
					break;
				case "trainer" : p = 2;
					break;
				case "user" : p = 1;
					break;
				default : p = 0;
				}				
				message.getValue().setPermission(p);
				System.out.println(message.toString());
				
			} catch (Exception e) {
				logger.info("Permission change failed. " + e.getMessage());
			}
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
