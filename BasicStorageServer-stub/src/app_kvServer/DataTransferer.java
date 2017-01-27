package app_kvServer;

import java.util.List;

import org.apache.log4j.Logger;

import app_kvServer.subscription.ClientSubscription;
import app_kvServer.subscription.SubscriptionMessageType;
import app_kvServer.subscription.SubscriptionReplicationController;
import common.logic.ByteArrayMath;
import common.logic.Communicator;
import common.logic.HashGenerator;
import common.logic.KVServerItem;
import common.logic.Value;
import common.messages.clientToServerMessage.KVMessageItem;
import common.messages.clientToServerMessage.KVMessage.KvStatusType;

/**
 * This class is responsible for data transfer from one server to another. It is called e.g. when
 * server gets ECS request to shut down. The whole data should than be transfered to other nodes.
 * It provides 2 public methods: transfer() and transferAllKeys(). In the first method the range of the 
 * transfered keys must be specified. Second method transfers all keys, for which server is currently responsible.
 *
 */
public class DataTransferer  {
	
	private KVServer server;
	private KVServerItem serverForNewData;
	private Logger logger;
	private Communicator communicator;
	
	public DataTransferer(KVServer server, KVServerItem serverForNewData) {
		this.server = server;
		this.logger = Logger.getRootLogger();
		this.serverForNewData = serverForNewData;
		this.communicator = new Communicator();
	}
	
	public void transfer( byte[] startIndex, byte[] endIndex) {
		for (String key: server.getServerStatusInformation().getKeys()) {
			if (ByteArrayMath.isValueBetweenTwoOthers(HashGenerator.generateHashFor(key), startIndex, endIndex)) {
				int numberOfVersions = server.getVersionController().getMaxVersionForKey(key);
				for (int i = 1; i <= numberOfVersions; i++) {
					sendMessageForVersion(key, i);
				}
				sendSubscriptionInformationForKey(key);
			}
		}
	}
	
	public void transferAllKeys() {
		for (String key: server.getServerStatusInformation().getKeys()) {
			int numberOfVersions = server.getVersionController().getMaxVersionForKey(key);
			for (int i = 1; i <= numberOfVersions; i++) {
				sendMessageForVersion(key, i);
			}
			sendSubscriptionInformationForKey(key);
		}
	}
	
	private void sendMessageForVersion(String key, int i) {
		String keyForVersionI = server.getVersionController().getKeyForVersion(key, i);
		Value value = server.getPersistenceLogic().get(keyForVersionI).getValue();
		KVMessageItem kvMessage = new KVMessageItem(KvStatusType.PUT, key, value);
		logger.info("Sending " + kvMessage.getKey() + ": " + kvMessage.getValue() +
				" to server " + serverForNewData.getName());
		communicator.sendMessage(serverForNewData, kvMessage);
	}
	
	private void sendSubscriptionInformationForKey(String key) {
		List<ClientSubscription> subscriptionList = server.getSubscriptionController().getSubscriptionListForKey(key);
		if (subscriptionList != null) {
			for (ClientSubscription subscription: subscriptionList) {
				SubscriptionReplicationController subscriptionReplicationController = new SubscriptionReplicationController(key, SubscriptionMessageType.ADD_SUBSCRIPTION, subscription, server);
				subscriptionReplicationController.sendSubscriptionToServer(serverForNewData);
			}
		}
	}
}
