package app_kvServer;

import java.util.ArrayList;
import java.util.List;

import app_kvServer.subscription.ClientSubscription;
import app_kvServer.subscription.SubscriptionMessageType;
import app_kvServer.subscription.SubscriptionReplicationController;
import common.logic.ByteArrayMath;
import common.logic.Communicator;
import common.logic.HashGenerator;
import common.logic.KVServerItem;
import common.messages.clientToServerMessage.KVMessage;
import common.messages.clientToServerMessage.KVMessageItem;
import common.messages.clientToServerMessage.KVMessage.KvStatusType;
import common.messages.ecsToServerMessage.ECSMessageItem;
import common.messages.ecsToServerMessage.ECSMessage.EcsStatusType;
import common.messages.serverToServerMessage.ServerToServerMessageItem;
import common.messages.serverToServerMessage.ServerToServerMessage.ServerToServerStatusType;

/**
 * This class is responsible for detection of the faulty servers. It sends ping messages to the servers, that follow current 
 * server in meta data table. If server does not respond, than message is sent to ECS with information about the faulty server.
 *
 */
public class FailureDetectorService implements Runnable {

	private static final int TIME_TO_NEXT_PING_MESSAGE = 10000;
	
	private List<KVServerItem> serversForInspection;
	private KVServer server;
	private boolean running;
	private Communicator communicator;
	private boolean sendingKeys = false;
	
	public FailureDetectorService(KVServer server) {
		this.server = server;
		this.running = true;
		this.communicator = new Communicator();
		this.serversForInspection = new ArrayList<>();
	}
	
	public void run() {	
		calculateServersForInspection();
		inspectServers();
	}
	
	private void calculateServersForInspection() {
		if (server.getMetaDataTable().size() > 2) {
			calculateTwoServersForInspection();
		} else if (server.getMetaDataTable().size() == 2) {
			calculateOneServerForInspection();
		} 
	}
	
	private void calculateTwoServersForInspection() {
		List<KVServerItem> metaDataTable = server.getMetaDataTable();
		int indexOfThisKVServerItem = metaDataTable.indexOf(server.getServerStatusInformation().getThisKvServerItem());	
		if (indexOfThisKVServerItem == 1) {
			this.serversForInspection.add(metaDataTable.get(indexOfThisKVServerItem - 1));
			this.serversForInspection.add(metaDataTable.get(metaDataTable.size() - 1));
		} else if (indexOfThisKVServerItem == 0 ) {
			this.serversForInspection.add(metaDataTable.get(metaDataTable.size() - 1));			
			this.serversForInspection.add(metaDataTable.get(metaDataTable.size() - 2));
		} else {
			this.serversForInspection.add(metaDataTable.get(indexOfThisKVServerItem - 1));			
			this.serversForInspection.add(metaDataTable.get(indexOfThisKVServerItem - 2));
		}
	}
	
	private void calculateOneServerForInspection() {
		this.serversForInspection = new ArrayList<>();
		if (server.getMetaDataTable().indexOf(server.getServerStatusInformation().getThisKvServerItem()) == 0) {
			this.serversForInspection.add(server.getMetaDataTable().get(1));
		} else {
			this.serversForInspection.add(server.getMetaDataTable().get(0));
		}
	}
	
	
	private void inspectServers() {
		while (this.running && server.getServerStatusInformation().isRunning()) {
			ServerToServerMessageItem pingMessage = new ServerToServerMessageItem(ServerToServerStatusType.GET_STATUS);
			this.sendingKeys = true;
			for (KVServerItem serverToInspect: this.serversForInspection) {
				ServerToServerMessageItem pingResponse = (ServerToServerMessageItem) communicator.sendMessage(serverToInspect, pingMessage);
				if (pingResponse == null) sendServerFailureToECS(serverToInspect);
			}
			this.sendingKeys = false;
			try {
				Thread.sleep(TIME_TO_NEXT_PING_MESSAGE);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private void sendServerFailureToECS(KVServerItem faultyServer) {
		running = false;
		ECSMessageItem serverFailureMessage = new ECSMessageItem(EcsStatusType.FAULTY_SERVER, faultyServer);
		ECSMessageItem responseMessage = (ECSMessageItem) communicator.sendMessage(server.getEcsMetaData(), serverFailureMessage);
		if (responseMessage !=null && responseMessage.getStatus().equals(EcsStatusType.REALLOCATE_DATA)) {
			running = true;
			reallocateDataFromServer(faultyServer);
			onMetaDataTableChange();
		} else if (responseMessage != null && responseMessage.getStatus().equals(EcsStatusType.SERVER_STOPPED)) {
			onMetaDataTableChange();
		}
	}
	
	public void onMetaDataTableChange() {
		while (!running && !sendingKeys)
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		calculateServersForInspection();
	}
	
	private void reallocateDataFromServer(KVServerItem faultyServer) {
		for (String key: server.getServerStatusInformation().getReplicationKeys()) {
			if (isKeyOfFaultyServer(key, faultyServer)) {
				sendKvPairToServerFromMetaDataTable(key);
				sendSubscriptionInformationForKey(key, faultyServer);
			}
		}
	}
	
	private boolean isKeyOfFaultyServer(String key, KVServerItem faultyServer) {
		return ByteArrayMath.isValueBetweenTwoOthers(HashGenerator.generateHashFor(key),
				faultyServer.getStartIndex(), faultyServer.getEndIndex());
	}
	
	private void sendKvPairToServerFromMetaDataTable(String key) {
		KVServerItem responsibleServer = findResponsibleServer(HashGenerator.generateHashFor(key));
		int numberOfVersions = server.getVersionController().getMaxVersionForKey(key);
		for (int i = 1; i <= numberOfVersions; i++) {
			responsibleServer = putKeyForVersion(key, responsibleServer, i);
		}
	}

	private KVServerItem putKeyForVersion(String key,KVServerItem responsibleServer, int version) {
		String keyForVersionI = server.getVersionController().getKeyForVersion(key, version);
		KVMessage getMessage = server.getPersistenceLogic().get(keyForVersionI);
		KVMessageItem putMessage = new KVMessageItem(KvStatusType.PUT, key, getMessage.getValue());
		KVMessageItem replyMessage = (KVMessageItem) communicator.sendMessage(responsibleServer, putMessage);
		while (!replyMessage.getStatus().equals(KvStatusType.PUT_SUCCESS)) {
			replyMessage = (KVMessageItem) communicator.sendMessage(responsibleServer, putMessage);
			if (replyMessage.getStatus().equals(KvStatusType.SERVER_NOT_RESPONSIBLE.toString())) responsibleServer = replyMessage.getServer();
		}
		return responsibleServer;
	}
	
	private void sendSubscriptionInformationForKey(String key, KVServerItem serverForNewData) {
		List<ClientSubscription> subscriptionList = server.getSubscriptionController().getSubscriptionListForKey(key);
		if (subscriptionList != null) {
			for (ClientSubscription subscription: subscriptionList) {
				SubscriptionReplicationController subscriptionReplicationController = new SubscriptionReplicationController(key, SubscriptionMessageType.ADD_SUBSCRIPTION, subscription, server);
				subscriptionReplicationController.sendSubscriptionToServer(serverForNewData);
			}
		}
	}
	
	public KVServerItem findResponsibleServer(byte[] value) {
		for (KVServerItem serverItem: server.getMetaDataTable()) {
			if (ByteArrayMath.isValueBetweenTwoOthers(value, serverItem.getStartIndex(), serverItem.getEndIndex())) {
				return serverItem;
			}
		}
		return null;
	}
}
