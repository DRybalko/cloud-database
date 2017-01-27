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
		this.serversForInspection = findNextServersInMetaDataTable(server.getMetaDataTable(),
				server.getServerStatusInformation().getThisKvServerItem(), 2);
	}
	
	/**
	 * Method that finds servers, that follow given server in metaDataTable (have higher end index).
	 * 
	 * @param serverItem for which following servers must be found
	 * @param numberOfServersToAdd number of servers to find
	 * @return list of KVServerItems that follow the given server
	 */
	private List<KVServerItem> findNextServersInMetaDataTable(List<KVServerItem> metaDataTable, KVServerItem serverItem, int numberOfServersToAdd) {
		List<KVServerItem> nextServers = new ArrayList<>();
		int indexOfServerItem = metaDataTable.indexOf(serverItem);
		if (numberOfServersToAdd > metaDataTable.size()) numberOfServersToAdd = metaDataTable.size();
		for (int i = 1; i <= numberOfServersToAdd; i++) {
			int indexOfNextServer = (indexOfServerItem + i) % metaDataTable.size();
			KVServerItem nextServer = metaDataTable.get(indexOfNextServer);
			if (nextServer.equals(serverItem)) break;
			nextServers.add(nextServer);
		}
		return nextServers;
	}
	
	private void inspectServers() {
		while (this.running && server.getServerStatusInformation().isRunning()) {
			ServerToServerMessageItem pingMessage = new ServerToServerMessageItem(ServerToServerStatusType.GET_STATUS);
			for (KVServerItem serverToInspect: this.serversForInspection) {
				ServerToServerMessageItem pingResponse = (ServerToServerMessageItem) communicator.sendMessage(serverToInspect, pingMessage);
				if (pingResponse == null) sendServerFailureToECS(serverToInspect);
			}
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
		if (running) {
			calculateServersForInspection();
		}
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
