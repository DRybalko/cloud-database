package app_kvServer;

import java.net.Socket;

import org.apache.log4j.Logger;

import app_kvServer.subscription.ClientSubscription;
import app_kvServer.subscription.SubscriptionInformer;
import app_kvServer.subscription.SubscriptionMessageType;
import app_kvServer.subscription.SubscriptionReplicationController;
import common.logic.ByteArrayMath;
import common.logic.HashGenerator;
import common.logic.KVServerItem;
import common.messages.clientToServerMessage.KVMessage;
import common.messages.clientToServerMessage.KVMessageItem;
import common.messages.clientToServerMessage.KVMessage.KvStatusType;

/**
 * This class is responsible for messages, that are received from client, like e.g. get, put, subscribe etc.
 */
public class KVMessageProcessor {

	private KVServer server;
	private Logger logger;
	private PutGetMessageProcessor messageProcessor;
	private Socket clientSocket;
	
	public KVMessageProcessor(Socket clientSocket, KVServer server) {
		this.server = server;
		this.logger = Logger.getRootLogger();
		this.messageProcessor = new PutGetMessageProcessor(server);
		this.clientSocket = clientSocket;
	} 
	
	public KVMessageItem processMessage(KVMessageItem message) {
		KVMessageItem replyMessage;
		if (message.getStatus().equals(KvStatusType.GET)) {
			replyMessage = messageProcessor.get(message);
		} else if (message.getStatus().equals(KvStatusType.GET_VERSION)){
			replyMessage = new KVMessageItem(KvStatusType.VERSION, server.getVersionController()
					.getMaxVersionForKey(message.getKey()));
		} else if (message.getStatus().equals(KvStatusType.PUT)) {
			replyMessage = processPutMessage(message);
		} else if (message.getStatus().equals(KvStatusType.PUT_REPLICATION))  {
			if (hasServerKVPair(message)) return new KVMessageItem(KvStatusType.PUT_SUCCESS);
			logger.info("Put replication message: " + message.getKey() + ", "+ message.getValue().getValue());
			addKeyToReplicationKeyList(message);
			replyMessage = messageProcessor.putKeyValue(message);
		} else if (message.getStatus().equals(KvStatusType.SUBSCRIBE)) {
			replyMessage = createSubscription(message);
		} else if (message.getStatus().equals(KvStatusType.UNSUBSCRIBE)) {
			replyMessage = removeSubscriptionIfResponsible(message);
		} else {
			replyMessage = new KVMessageItem(KvStatusType.ERROR);
		}
		return replyMessage;
	}

	private KVMessageItem createSubscription(KVMessageItem message) {
		if (isServerResponsibleForSubscription(message.getKey())) {
			return addSubscription(message);
		} else if (!server.getServerStatusInformation().getKeys().contains(message.getKey())){
			return new KVMessageItem(KvStatusType.KEY_NOT_EXISTS);
		} else {
			return sendNotResponsibleMessage(message);
		}
	}
	
	private KVMessageItem addSubscription(KVMessageItem message) {
		ClientSubscription subscription = new ClientSubscription(clientSocket.getRemoteSocketAddress(), message.getPort());
		sendSubscriptionStatusToOtherServers(message, SubscriptionMessageType.ADD_SUBSCRIPTION, subscription);
		server.getSubscriptionController().addSubscription(message.getKey(), subscription);
		return new KVMessageItem(KvStatusType.SUBSCRIPTION_SUCCESS);
	}
	
	private void sendSubscriptionStatusToOtherServers(KVMessageItem message, SubscriptionMessageType subscriptionType, ClientSubscription subscription) {
		SubscriptionReplicationController subscriptionReplicationController
			= new SubscriptionReplicationController(message.getKey(), subscriptionType, subscription, server);
		new Thread(subscriptionReplicationController).start();
	}
	
	private KVMessageItem removeSubscriptionIfResponsible(KVMessageItem message) {
		if (ByteArrayMath.isValueBetweenTwoOthers(HashGenerator.generateHashFor(message.getKey()),
				server.getServerStatusInformation().getStartIndex(), server.getServerStatusInformation().getEndIndex())) {
			ClientSubscription subscription = new ClientSubscription(clientSocket.getRemoteSocketAddress(), message.getPort());
			sendSubscriptionStatusToOtherServers(message, SubscriptionMessageType.DELETE_SUBSCRIPTION, subscription);
			return removeSubscription(message);
		} else {
			return sendNotResponsibleMessage(message);
		}
	} 
	
	private KVMessageItem removeSubscription(KVMessageItem message) {
		ClientSubscription subscription = new ClientSubscription(clientSocket.getRemoteSocketAddress(), message.getPort());
		server.getSubscriptionController().removeSubscription(message.getKey(), subscription);
		return new KVMessageItem(KvStatusType.UNSUBSCRIBE_SUCCESS);
	}
	
	private boolean isServerResponsibleForSubscription(String key) {
		return server.getServerStatusInformation().getKeys().contains(key) 
				&& ByteArrayMath.isValueBetweenTwoOthers(HashGenerator.generateHashFor(key),
						server.getServerStatusInformation().getStartIndex(), server.getServerStatusInformation().getEndIndex());
	}
	
	private KVMessageItem processPutMessage(KVMessageItem message) {
		if (ByteArrayMath.isValueBetweenTwoOthers(HashGenerator.generateHashFor(message.getKey()),
				server.getServerStatusInformation().getStartIndex(), server.getServerStatusInformation().getEndIndex())) {	
			logger.info("Put message: " + message.getKey() + ", "+ message.getValue().getValue());
			if (hasServerKVPair(message)) return new KVMessageItem(KvStatusType.PUT_SUCCESS);
			addKeyToKeyList(message);
			informSubscribers(message);
			return messageProcessor.putKeyValue(message);
		} else {
			return sendNotResponsibleMessage(message);
		}
	}

	private boolean hasServerKVPair(KVMessageItem message) {
		return server.getVersionController().hasKey(message.getKey(), message.getValue().getTimestamp());
	}
	
	private KVMessageItem sendNotResponsibleMessage(KVMessageItem message) {
		KVServerItem responsibleServer = findResponsibleServer(HashGenerator.generateHashFor(message.getKey()));
		KVMessageItem responseMessage =  new KVMessageItem(KvStatusType.SERVER_NOT_RESPONSIBLE);
		responseMessage.setServer(responsibleServer);
		return responseMessage;
	}
	
	private void informSubscribers(KVMessageItem message) {
		if (server.getSubscriptionController().isKeySubscribedByClient(message.getKey())) {
			SubscriptionInformer subscriptionInformer = new SubscriptionInformer(message, server.getSubscriptionController());
			new Thread(subscriptionInformer).start();
		}
		if (message.getValue().getValue().trim().equals("null")) {
			sendSubscriptionStatusToOtherServers(message, SubscriptionMessageType.DELETE_KEY, null);
			server.getSubscriptionController().deleteKey(message.getKey());
		}
	}
	
	private void addKeyToKeyList(KVMessage receivedMessage) {
		if (!receivedMessage.getValue().getValue().equals("null")) {
			server.getServerStatusInformation().addKey(receivedMessage.getKey());
		} else {
			server.getServerStatusInformation().removeKey(receivedMessage.getKey());
		}
	}
	
	private void addKeyToReplicationKeyList(KVMessage receivedMessage) {
		if (!receivedMessage.getValue().getValue().equals("null")) {
			server.getServerStatusInformation().addReplicationKey(receivedMessage.getKey());
		} else {
			server.getServerStatusInformation().removeReplicationKey(receivedMessage.getKey());
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