package app_kvServer;

import org.apache.log4j.Logger;

import app_kvServer.subscription.ClientSubscription;
import common.messages.serverToServerMessage.ServerToServerMessageItem;
import common.messages.serverToServerMessage.ServerToServerMessage.ServerToServerStatusType;

/**
 * Messages, that are received from another server are processed in this class. These messages 
 * can be e.g ping message or subscription replication.
 */
public class ServerToServerMessageProcessor {

	private KVServer server;
	private Logger logger;
	
	public ServerToServerMessageProcessor(KVServer server) {
		this.server = server;
		this.logger = Logger.getRootLogger();
	}
	
	public ServerToServerMessageItem processMessage(ServerToServerMessageItem message) {
		ServerToServerMessageItem replyMessage;
		if (message.getStatus().equals(ServerToServerStatusType.GET_STATUS)){
			replyMessage = new ServerToServerMessageItem(ServerToServerStatusType.IN_PROGRESS);
			logger.debug("Got Ping message from another server");
		} else if (message.getStatus().equals(ServerToServerStatusType.SUBSCRIBE)) {
			replyMessage = addSubscription(message);
		} else if (message.getStatus().equals(ServerToServerStatusType.UNSUBSCRIBE)) {
			replyMessage = removeSubscription(message);
		} else if (message.getStatus().equals(ServerToServerStatusType.DELETE_SUBSCRIPTION_KEY)) {
			server.getSubscriptionController().deleteKey(message.getKey());
			replyMessage = new ServerToServerMessageItem(ServerToServerStatusType.DELETE_SUCCESS);
		} else {
			replyMessage = new ServerToServerMessageItem(ServerToServerStatusType.ERROR);
		}
		return replyMessage;
	}
	
	private ServerToServerMessageItem addSubscription(ServerToServerMessageItem message) {
		ClientSubscription subscription = new ClientSubscription(message.getIp(), message.getPort());
		server.getSubscriptionController().addSubscription(message.getKey(), subscription);
		return new ServerToServerMessageItem(ServerToServerStatusType.SUBSCRIPTION_SUCCESS);
	}
	
	private ServerToServerMessageItem removeSubscription(ServerToServerMessageItem message) {
		ClientSubscription subscription = new ClientSubscription(message.getIp(), message.getPort());
		server.getSubscriptionController().removeSubscription(message.getKey(), subscription);
		return new ServerToServerMessageItem(ServerToServerStatusType.UNSUBSCRIBE_SUCCESS);
	}
}
