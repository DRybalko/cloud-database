package app_kvServer.subscription;

import app_kvServer.KVServer;
import common.logic.Communicator;
import common.logic.KVServerItem;
import common.messages.serverToServerMessage.ServerToServerMessageItem;
import common.messages.serverToServerMessage.ServerToServerMessage.ServerToServerStatusType;

/**
 * In order to provide notification mechanism when server responsible for the key is stopped or does not reply, 
 * information about subscribers is saved on each server in the system. Each time, when server gets request for subscription
 * the information about the subscriber is sent to all servers.
 *
 */
public class SubscriptionReplicationController implements Runnable {
	
	private String key;
	private SubscriptionMessageType messageType;
	private KVServer server;
	private Communicator communicator;
	private ClientSubscription subscription;
	
	public SubscriptionReplicationController(String key, SubscriptionMessageType messageType, ClientSubscription subscription, KVServer server) {
		this.key = key;
		this.messageType = messageType;
		this.server = server;
		this.communicator = new Communicator();
		this.subscription = subscription;
	}
	
	public void run() {
		if (messageType.equals(SubscriptionMessageType.ADD_SUBSCRIPTION)) {
			ServerToServerMessageItem subscriptionMessage = new ServerToServerMessageItem(ServerToServerStatusType.SUBSCRIBE, key, subscription.getPort(), subscription.getIp());
			sendSubscriptionStatusToAllServers(subscriptionMessage, ServerToServerStatusType.SUBSCRIPTION_SUCCESS);
		} else if (messageType.equals(SubscriptionMessageType.DELETE_SUBSCRIPTION)) {
			ServerToServerMessageItem subscriptionMessage = new ServerToServerMessageItem(ServerToServerStatusType.UNSUBSCRIBE, key, subscription.getPort(), subscription.getIp());
			sendSubscriptionStatusToAllServers(subscriptionMessage, ServerToServerStatusType.UNSUBSCRIBE_SUCCESS);
		} else if (messageType.equals(SubscriptionMessageType.DELETE_KEY)) {
			ServerToServerMessageItem subscriptionMessage = new ServerToServerMessageItem(ServerToServerStatusType.DELETE_SUBSCRIPTION_KEY);
			subscriptionMessage.setKey(key);
			sendSubscriptionStatusToAllServers(subscriptionMessage, ServerToServerStatusType.DELETE_SUCCESS);
		}
	}

	private void sendSubscriptionStatusToAllServers(ServerToServerMessageItem subscriptionMessage, ServerToServerStatusType responseMessageType) {
		for (KVServerItem serverItem: server.getMetaDataTable()) {
			if (!serverItem.equals(server.getServerStatusInformation().getThisKvServerItem())) {
				ServerToServerMessageItem reply = (ServerToServerMessageItem) communicator.sendMessage(serverItem, subscriptionMessage);
			}
		}
	}
	
	public void sendSubscriptionToServer(KVServerItem server) {
		ServerToServerMessageItem subscriptionMessage = new ServerToServerMessageItem(ServerToServerStatusType.SUBSCRIBE, key, subscription.getPort(), subscription.getIp());
		communicator.sendMessage(server, subscriptionMessage);
	}
}
