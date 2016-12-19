package app_kvServer;

import org.apache.log4j.Logger;

import common.logic.ByteArrayMath;
import common.logic.HashGenerator;
import common.logic.KVServerItem;
import common.messages.KVMessage;
import common.messages.KVMessageItem;
import common.messages.KVMessage.KvStatusType;

public class KVMessageProcessor {

	private KVServer server;
	private Logger logger;
	
	public KVMessageProcessor(KVServer server) {
		this.server = server;
		this.logger = Logger.getRootLogger();
	}
	
	public KVMessageItem processMessage(KVMessageItem message) {
		KVMessageItem replyMessage;
		if (message.getStatus().equals(KvStatusType.GET)) {
			replyMessage = (KVMessageItem) server.getPersistenceLogic().get(message.getKey());
		} else if (message.getStatus().equals(KvStatusType.PUT)) {
			replyMessage = processPutMessage(message);
		} else if (message.getStatus().equals(KvStatusType.PUT_REPLICATION))  {
			addKeyToKeyList(message);
			replyMessage = (KVMessageItem) server.getPersistenceLogic().put(message.getKey(), message.getValue());
		} else {
			logger.error("Unnown message status, can not be proceeded.");
			replyMessage = new KVMessageItem(KvStatusType.ERROR);
		}
		return replyMessage;
	}

	private KVMessageItem processPutMessage(KVMessageItem message) {
		if (ByteArrayMath.isValueBetweenTwoOthers(HashGenerator.generateHashFor(message.getKey()),
				server.getServerStatusInformation().getStartIndex(), server.getServerStatusInformation().getEndIndex())) {	
			addKeyToKeyList(message);
			sendReplication(message);
			return (KVMessageItem) server.getPersistenceLogic().put(message.getKey(), message.getValue());
		} else {
			return sendNotResponsibleMessage(message);
		}
	}
	
	private void sendReplication(KVMessageItem message) {
		KVMessageItem putReplication = new KVMessageItem(KvStatusType.PUT_REPLICATION, message.getKey(), message.getValue());
		server.getReplicaCoordinator().put(putReplication);
	}
	
	private KVMessageItem sendNotResponsibleMessage(KVMessageItem message) {
		KVServerItem responsibleServer = findResponsibleServer(HashGenerator.generateHashFor(message.getKey()));
		KVMessageItem responseMessage =  new KVMessageItem(KvStatusType.SERVER_NOT_RESPONSIBLE);
		responseMessage.setServer(responsibleServer);
		return responseMessage;
	}
	
	private void addKeyToKeyList(KVMessage receivedMessage) {
		logger.debug("Check if received message is put");
		if ((receivedMessage.getStatus().equals(KvStatusType.PUT) || receivedMessage.getStatus().equals(KvStatusType.PUT_REPLICATION)) 
				&& !receivedMessage.getValue().equals("null")) {
			logger.debug("PUT message");
			server.addKey(receivedMessage.getKey());
		} else if ((receivedMessage.getStatus().equals(KvStatusType.PUT) || receivedMessage.getStatus().equals(KvStatusType.PUT_REPLICATION))
				&& receivedMessage.getValue().equals("null")) {
			logger.debug("DELETE message");
			server.deleteKey(receivedMessage.getKey());
		}
	}
	
	private KVServerItem findResponsibleServer(byte[] keyHash) {
		for (KVServerItem server: server.getMetaDataTable()) {
			if (ByteArrayMath.isValueBetweenTwoOthers(keyHash, server.getStartIndex(), server.getEndIndex())) {
				return server;
			}
		}
		return null;
	}
	
}
