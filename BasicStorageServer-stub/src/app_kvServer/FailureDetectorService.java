package app_kvServer;

import java.util.ArrayList;
import java.util.List;

import common.logic.ByteArrayMath;
import common.logic.Communicator;
import common.logic.HashGenerator;
import common.logic.KVServerItem;
import common.messages.ECSMessage.EcsStatusType;
import common.messages.ECSMessageItem;
import common.messages.KVMessageItem;
import common.messages.KVMessage.KvStatusType;
import common.messages.PingMessage.PingStatusType;
import common.messages.PingMessageItem;

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
			PingMessageItem pingMessage = new PingMessageItem(PingStatusType.GET_STATUS);
			for (KVServerItem serverToInspect: this.serversForInspection) {
				PingMessageItem pingResponse = (PingMessageItem) communicator.sendMessage(serverToInspect, pingMessage);
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
		if (responseMessage.getStatus().equals(EcsStatusType.REALLOCATE_DATA)) {
			running = true;
			reallocateDataFromServer(faultyServer);
			onMetaDataTableChange();
		} else if (responseMessage.getStatus().equals(EcsStatusType.SERVER_STOPPED)) {
			onMetaDataTableChange();
		}
	}
	
	public void onMetaDataTableChange() {
		if (running) {
			calculateServersForInspection();
		}
	}
	
	private void reallocateDataFromServer(KVServerItem faultyServer) {
		for (String key: server.getServerStatusInformation().getKeys()) {
			if (isKeyOfFaultyServer(key, faultyServer)) {
				sendKvPairToServerFromMetaDataTable((KVMessageItem) server.getPersistenceLogic().get(key));
			}
		}
	}
	
	private boolean isKeyOfFaultyServer(String key, KVServerItem faultyServer) {
		return ByteArrayMath.isValueBetweenTwoOthers(HashGenerator.generateHashFor(key),
				faultyServer.getStartIndex(), faultyServer.getEndIndex());
	}
	
	private void sendKvPairToServerFromMetaDataTable(KVMessageItem kvMessage) {
		KVServerItem responsibleServer = findResponsibleServer(HashGenerator.generateHashFor(kvMessage.getKey()));
		kvMessage.setStatus(KvStatusType.PUT);
		KVMessageItem replyMessage = (KVMessageItem) communicator.sendMessage(responsibleServer, kvMessage);
		while (!replyMessage.getStatus().equals(KvStatusType.PUT_SUCCESS.toString())) {
			replyMessage = (KVMessageItem) communicator.sendMessage(responsibleServer, kvMessage);
			if (replyMessage.getStatus().equals(KvStatusType.SERVER_NOT_RESPONSIBLE.toString())) responsibleServer = replyMessage.getServer();
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
