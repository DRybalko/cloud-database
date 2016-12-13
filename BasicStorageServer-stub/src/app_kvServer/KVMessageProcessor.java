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
		
			if (message.getStatus().equals(KvStatusType.GET)) {
				KVMessageItem getResult = (KVMessageItem) server.getPersistenceLogic().get(message.getKey());
				return getResult;
			} else if (message.getStatus().equals(KvStatusType.PUT)) {
				if (ByteArrayMath.isValueBetweenTwoOthers(HashGenerator.generateHashFor(message.getKey()),
						server.getServerStatusInformation().getStartIndex(), server.getServerStatusInformation().getEndIndex())) {	
					addKeyToKeyList(message);
					sendReplication(message);
					return (KVMessageItem) server.getPersistenceLogic().put(message.getKey(), message.getValue());
				} else {
					return sendNotResponsibleMessage(message);
				}
			} else if (message.getStatus().equals(KvStatusType.PUT_REPLICATION))  {
				addKeyToKeyList(message);
				return (KVMessageItem) server.getPersistenceLogic().put(message.getKey(), message.getValue());
			} else {
				logger.error("Unnown message status, can not be proceeded.");
				return null;
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
			logger.debug("Message is put");
			server.addKey(receivedMessage.getKey());
		} else if ((receivedMessage.getStatus().equals(KvStatusType.PUT) || receivedMessage.getStatus().equals(KvStatusType.PUT_REPLICATION))
				&& receivedMessage.getValue().equals("null")) {
			logger.debug("Message is delete");
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
