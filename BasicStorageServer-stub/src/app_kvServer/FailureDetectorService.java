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
