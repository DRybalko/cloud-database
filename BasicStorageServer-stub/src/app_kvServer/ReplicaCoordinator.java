package app_kvServer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import common.logic.Communicator;
import common.logic.KVServerItem;
import common.messages.KVMessage;
import common.messages.KVMessageItem;
import common.messages.KVMessage.KvStatusType;

public class ReplicaCoordinator {
	
	private List<KVServerItem> replicas;
	private KVServer server;
	private Communicator communicator;
	
	public ReplicaCoordinator(KVServer server) {
		Logger.getRootLogger().debug("Initialize replica coordinator");
		this.server = server;
		this.communicator = new Communicator();
		this.replicas = findServersForReplication();
	}
	
	private List<KVServerItem> findServersForReplication() {
		if (server.getMetaDataTable().size() == 2) {
			return addSingleServerToReplicas();
		} else if (server.getMetaDataTable().size() >= 3) {
			return addTwoServersToReplicas();
		} return Collections.emptyList();
	}
	
	private List<KVServerItem> addSingleServerToReplicas() {
		List<KVServerItem> replicasList = new ArrayList<>();
		List<KVServerItem> metaDataTable = server.getMetaDataTable();
		if (metaDataTable.indexOf(server.getServerStatusInformation().getThisKvServerItem()) == 0) {
			replicasList.add(metaDataTable.get(1));
		} else {
			replicasList.add(metaDataTable.get(0));
		}
		return replicasList;
	}

	private List<KVServerItem> addTwoServersToReplicas() {
		List<KVServerItem> replicasList = new ArrayList<>();
		List<KVServerItem> metaDataTable = server.getMetaDataTable();
		int thisServerIndexInMetaDataTable = metaDataTable.indexOf(server.getServerStatusInformation().getThisKvServerItem());
		int nextServerIndex = (thisServerIndexInMetaDataTable + 1) % metaDataTable.size();
		replicasList.add(metaDataTable.get(nextServerIndex));
		int secondNextServerIndex = (thisServerIndexInMetaDataTable + 2) % metaDataTable.size();
		replicasList.add(metaDataTable.get(secondNextServerIndex));
		return replicasList;
	}
	
	public void put(KVMessageItem message) {
		for (KVServerItem serverItem: replicas) {
			communicator.sendMessage(serverItem, message);
		}
	}
	
	public void updateReplicas() {
		Logger.getRootLogger().debug("Update replicas in replica coordinator");
		List<KVServerItem> newReplicationServers = findServersForReplication();
		if (newReplicationServers.size() == 1) {
			sendServerDataToReplication(newReplicationServers.get(0));
		} else if (newReplicationServers.size() > 1) {
			updateTwoReplicationServers(newReplicationServers);
		}
	}
	
	private void updateTwoReplicationServers(List<KVServerItem> newReplicationServers) {
		if (replicas.size() == 1) {
			updateReplicasAfterAddingSecondServer(newReplicationServers);
		} else if (replicas.get(0).getPort().equals(newReplicationServers.get(0).getPort()) &&
				!replicas.get(1).getPort().equals(newReplicationServers.get(1).getPort())) {
				deleteReplicationFromServer(replicas.get(1));
				sendServerDataToReplication(newReplicationServers.get(1));
		} else if (replicas.get(0).getPort().equals(newReplicationServers.get(1).getPort())) {
				deleteReplicationFromServer(replicas.get(1));
				sendServerDataToReplication(newReplicationServers.get(0));
		} else if (replicas.get(1).getName().equals(newReplicationServers.get(0).getName())) {
				deleteReplicationFromServer(replicas.get(0));
				sendServerDataToReplication(newReplicationServers.get(1));
		}
		replicas = newReplicationServers;
	}
	
	private void updateReplicasAfterAddingSecondServer(List<KVServerItem> newReplicationServers) {
		if (replicas.get(0).getPort().equals(newReplicationServers.get(0).getPort())) sendServerDataToReplication(newReplicationServers.get(1));
		else if (replicas.get(0).getPort().equals(newReplicationServers.get(1).getPort())) sendServerDataToReplication(newReplicationServers.get(0));
		else {
			deleteReplicationFromServer(replicas.get(0));
			sendServerDataToReplication(newReplicationServers.get(0));
			sendServerDataToReplication(newReplicationServers.get(1));
		}
	}
	
	private void deleteReplicationFromServer(KVServerItem serverItem) {
		if (communicator.checkStarted(serverItem)) {
			for (String key: server.getServerStatusInformation().getKeys()) {
				KVMessageItem deleteMessage = new KVMessageItem(KvStatusType.PUT_REPLICATION, key, "null");
				communicator.sendMessage(serverItem, deleteMessage);
			}
		}
	}

	private void sendServerDataToReplication(KVServerItem serverItem) {
		for (String key: server.getServerStatusInformation().getKeys()) {
			KVMessage getMessage = server.getPersistenceLogic().get(key);
			KVMessageItem putMessage = new KVMessageItem(KvStatusType.PUT_REPLICATION, key, getMessage.getValue());
			communicator.sendMessage(serverItem, putMessage);
		}
	}       
	
	public void deleteAllReplications() {
		for (KVServerItem serverItem: replicas) {
			deleteReplicationFromServer(serverItem);
		}
	}
}
