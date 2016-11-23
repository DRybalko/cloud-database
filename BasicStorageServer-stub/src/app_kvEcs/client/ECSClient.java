package app_kvEcs.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvClient.CommandLineProcessor;
import app_kvEcs.logic.ECSLogic;
import app_kvEcs.logic.Repository;
import app_kvServer.EcsConnection;
import app_kvServer.KVServer;
import client.KVCommInterface;
import client.KVStore;
import common.logic.KVServerItem;
import common.messages.KVMessage;

public class ECSClient {

	private static String[] input;
	private static Logger logger = Logger.getRootLogger();
	private static boolean quit;
	private static KVServerItem server;
	private static KVServer kv;
	private static ECSLogic ecs;
	private static Repository repository;



	public static void readAndProcessInput() {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		while (!quit) {
			readInputLine(br);
		}
	}
	
	private static void readInputLine(BufferedReader br) {

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
		case "init": kv.initializeServer();
						break;
		case "start": kv.start();
						break;
		case "stop": kv.stop();
						break;
		case "shutDown": kv.shutDown();
						break;
		case "lockWrite": kv.lockWrite();
						break;
		case "unLockWrite": kv.unLockWrite();
						break;
		case "moveData": kv.moveData(server.getStartIndex(), server.getEndIndex());
						break;
		case "update": ecs.updateMetaDataTableOfWorkingServers();
						break;
		
		default: errorMessage();
		}
	}
	
	private static void errorMessage() {
		System.out.println("Error. Unknown command.");	
	};

	public static void main(String[] args) {
		//TODO will be implemented later
	
		ecs = new ECSLogic(repository);
		readAndProcessInput();
	}
}
