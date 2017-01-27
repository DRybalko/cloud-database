package app_kvServer;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import common.logic.Communicator;
import common.logic.KVServerItem;
import common.logic.Value;
import common.messages.clientToServerMessage.KVMessage;
import common.messages.clientToServerMessage.KVMessageItem;
import common.messages.clientToServerMessage.KVMessage.KvStatusType;

/**
 * This class is responsible for communication with servers, on which data of the current server must
 * be replicated. In our system these are two servers, that follow current server in meta data table.
 *
 */
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
		return findNextServersInMetaDataTable(server.getMetaDataTable(), 
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
	
	public void deleteAllReplications() {
		for (KVServerItem serverItem: replicas) {
			deleteReplicationFromServer(serverItem);
		}
	}
	
	private void deleteReplicationFromServer(KVServerItem serverItem) {
		if (communicator.checkStarted(serverItem)) {
			for (String key: server.getServerStatusInformation().getKeys()) {
				KVMessageItem deleteMessage = new KVMessageItem(KvStatusType.PUT_REPLICATION, key, new Value(0, LocalDateTime.now(), "null"));
				communicator.sendMessage(serverItem, deleteMessage);
			}
		}
	}

	private void sendServerDataToReplication(KVServerItem serverItem) {
		for (String key: server.getServerStatusInformation().getKeys()) {
			int numberOfVersions = server.getVersionController().getMaxVersionForKey(key);
			for (int i = 1; i <= numberOfVersions; i++) {
				String keyForVersionI = server.getVersionController().getKeyForVersion(key, i);
				KVMessage getMessage = server.getPersistenceLogic().get(keyForVersionI);
				KVMessageItem putMessage = new KVMessageItem(KvStatusType.PUT_REPLICATION, key, getMessage.getValue());
				communicator.sendMessage(serverItem, putMessage);
			}		
		}
	}       

}
