package app_kvServer;

import org.apache.log4j.Logger;

import common.logic.ByteArrayMath;
import common.logic.Communicator;
import common.logic.HashGenerator;
import common.logic.KVServerItem;
import common.messages.KVMessage.KvStatusType;
import common.messages.KVMessageItem;

public class DataTransferer implements Runnable {
	
	private KVServer server;
	private byte[] startIndex;
	private byte[] endIndex;
	private KVServerItem serverForNewData;
	private Logger logger;
	
	public DataTransferer(KVServer server, byte[] startIndex, byte[] endIndex, KVServerItem serverForNewData) {
		this.server = server;
		this.logger = Logger.getRootLogger();
		this.startIndex = startIndex;
		this.endIndex = endIndex;
		this.serverForNewData = serverForNewData;
	}
	
	public void run() {
		Communicator communicator = new Communicator();
		for (String key: server.getServerStatusInformation().getKeys()) {
			if (ByteArrayMath.isValueBetweenTwoOthers(HashGenerator.generateHashFor(key), startIndex, endIndex)) {
				String value = server.getPersistenceLogic().get(key).getValue();
				KVMessageItem kvMessage = new KVMessageItem(KvStatusType.PUT, key, value);
				logger.info("Sending " + kvMessage.getKey() + ": " + kvMessage.getValue() +
						" to server " + serverForNewData.getName());
				KVMessageItem reply = (KVMessageItem) communicator.sendMessage(serverForNewData, kvMessage);
			}
		}
	}
}
