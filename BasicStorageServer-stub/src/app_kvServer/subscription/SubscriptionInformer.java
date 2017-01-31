package app_kvServer.subscription;

import common.logic.Communicator;
import common.logic.KVServerItem;
import common.messages.clientToServerMessage.KVMessage;
import common.messages.clientToServerMessage.KVMessageItem;
import common.messages.clientToServerMessage.KVMessage.KvStatusType;

/**
 * This class implements interface Runnable and therefore started in the separate thread. Each time, when key, on which 
 * there are subscribers, is updated or deleted, this class sends messages to all subscribers for this key.
 */
public class SubscriptionInformer implements Runnable {

	private KVMessageItem message;
	private SubscriptionController subscriptionController;
	private Communicator communicator;
	
	public SubscriptionInformer(KVMessageItem message, SubscriptionController subscriptionController) {
		this.message = message;
		this.subscriptionController = subscriptionController;
		this.communicator = new Communicator();
	}
	
	public void run() {
		for (ClientSubscription client: subscriptionController.getSubscriptionListForKey(message.getKey())) {
			KVServerItem serverItem = new KVServerItem("client" +client.getIp() + client.getPort(), client.getIp(), client.getPort());
			KVMessageItem updateMessage = new KVMessageItem(KvStatusType.PUT, message.getKey(), message.getValue());
			communicator.sendMessage(serverItem, updateMessage);
		}
	}
}
