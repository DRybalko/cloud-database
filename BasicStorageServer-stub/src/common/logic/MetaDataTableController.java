package common.logic;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import org.apache.log4j.Logger;

/**
 * Class provides all functionality for metadata table. It creates a new table based on list with 
 * available servers. Table is a list of KVServerItem elements that are saved
 * in a sequence based on corresponding hash value. The element with the lowest hash value is on the first list position. 
 */
public class MetaDataTableController {

	private Logger logger;
	private LinkedList<KVServerItem> metaDataTable;
	private List<KVServerItem> availableServers;
	
	public MetaDataTableController(List<KVServerItem> availableServers) {
		this.logger = Logger.getRootLogger();
		this.metaDataTable = new LinkedList<>();
		this.availableServers = availableServers;
	}
	
	/**
	 * Specified number of servers from available servers are initialized and added to the metadata table.
	 * @param numberOfNodes number of servers to add to the metadata table
	 * @return metadata table
	 */
	public List<KVServerItem> initializeTable(int numberOfNodes) {
		Iterator<KVServerItem> serversIterator = availableServers.iterator();
		for (int i = 0; i < numberOfNodes; i++) {
			if (serversIterator.hasNext()) {
				KVServerItem server = serversIterator.next();
				addServerToMetaData(server);
			} else {
				logger.error("Number of nodes to instantiate is greate then number of servers available in repository. No servers were instanciated");
			}
		}
		return metaDataTable;
	}
	
	/**
	 * New server is added to the metadata table on the position based on the server hash value.
	 * @param server to add to the metadata table
	 * @return new server in the metadata table
	 */
	public KVServerItem addServerToMetaData(KVServerItem server) {
		KVServerItem foundServer = checkServerInTable(server);
		if (foundServer != null) return foundServer;
		byte[] serverEndIndex = HashGenerator.generateHashForValues(server.getIp(), server.getPort());
		server.setEndIndex(serverEndIndex);
		if (metaDataTable.isEmpty()) {
			addFirstElementToEmptyMetaData(server);
			return null;
		}
		else if (metaDataTable.size() == 1) return addServerToMetaDataTableWithOneElement(server);
		else return addServerToMetaDataTableWithMoreThanOneElement(server);
	}
	
	private KVServerItem checkServerInTable(KVServerItem server) {
		KVServerItem foundServer = hasServer(server);
		if (foundServer != null) {
			foundServer.setStartIndex(server.getStartIndex());
			return foundServer;
		}
		return null;
	}
	
	private KVServerItem hasServer(KVServerItem server) {
		for (KVServerItem serverItem: metaDataTable) {
			if (serverItem.getName().equals(server.getName()) 
					&& serverItem.getIp().equals(server.getIp())
					&& serverItem.getPort().equals(server.getPort())) {
				return serverItem;
			}
		}
		return null;	
	}
	
	private void addFirstElementToEmptyMetaData(KVServerItem server) {
		byte[] serverStartIndex = ByteArrayMath.increment(server.getEndIndex());
		server.setStartIndex(serverStartIndex);
		metaDataTable.add(server);
	}
	
	private KVServerItem addServerToMetaDataTableWithOneElement(KVServerItem server) {
		KVServerItem existingNode = metaDataTable.get(0);
		if (ByteArrayMath.compareByteArrays(server.getEndIndex(), existingNode.getStartIndex()) < 0) {
			return insertAtPosition(server, existingNode, 0);
		} else {
			return insertAtPosition(server, existingNode, 1);
		}
	}
	
	private KVServerItem insertAtPosition(KVServerItem serverToInsert, KVServerItem existingNode, int position) {
		serverToInsert.setStartIndex(ByteArrayMath.increment(existingNode.getEndIndex()));
		existingNode.setStartIndex(ByteArrayMath.increment(serverToInsert.getEndIndex()));
		metaDataTable.add(position, serverToInsert);
		return existingNode;
	}

	private KVServerItem addServerToMetaDataTableWithMoreThanOneElement(KVServerItem server) {
		ListIterator<KVServerItem> iterator = metaDataTable.listIterator();
		KVServerItem previousNode = iterator.next();
		KVServerItem nextNode;
		while (iterator.hasNext()) {
			nextNode = iterator.next();
			if (ByteArrayMath.isValueBetweenTwoOthers(server.getEndIndex(), previousNode.getEndIndex(), nextNode.getEndIndex())) {
				server.setStartIndex(ByteArrayMath.increment(previousNode.getEndIndex()));
				nextNode.setStartIndex(ByteArrayMath.increment(server.getEndIndex()));
				metaDataTable.add(iterator.previousIndex(), server);
				return nextNode;
			}
			previousNode = nextNode;
		}
		server.setStartIndex(ByteArrayMath.increment(metaDataTable.get(metaDataTable.size()-1).getEndIndex()));
		metaDataTable.get(0).setStartIndex(ByteArrayMath.increment(server.getEndIndex()));
		if (((byte) server.getEndIndex()[0]) < 0)  {
			metaDataTable.add(server);
		} else {
			metaDataTable.add(0, server);
		}
		return metaDataTable.get(0);
	}

	/**
	 * Method to remove server from meta data table 
	 * @param server to be removed
	 * @return neighbor server, whose start index must be updated
	 */
	public KVServerItem removeServerFromMetaData(KVServerItem server) {
		KVServerItem neighborServer = null;
		int serverToRemoveIndex = metaDataTable.indexOf(server);
		ListIterator<KVServerItem> iterator = metaDataTable.listIterator(serverToRemoveIndex);
		if (metaDataTable.size() == 2) {
			if (iterator.hasNext()) {
				neighborServer = iterator.next();
			} else {
				neighborServer = iterator.previous();
			}
			neighborServer.setStartIndex(ByteArrayMath.increment(neighborServer.getEndIndex()));
		} else if (metaDataTable.size() > 2){
			if (serverToRemoveIndex == 0) {
				neighborServer = iterator.next();
				neighborServer.setStartIndex(ByteArrayMath.increment(metaDataTable.getLast().getEndIndex()));
			} else if (serverToRemoveIndex == metaDataTable.size() - 1) {
				KVServerItem nextToLast = iterator.previous();
				neighborServer = metaDataTable.getFirst();
				neighborServer.setStartIndex(ByteArrayMath.increment(nextToLast.getEndIndex()));
			} else {
				KVServerItem previousServer = iterator.previous();
				iterator.next();
				neighborServer = iterator.next();
				neighborServer.setStartIndex(ByteArrayMath.increment(previousServer.getEndIndex()));
				
			}
		}
		metaDataTable.remove(server);
		return neighborServer;
	}
	

	public LinkedList<KVServerItem> getMetaDataTable() {
		return this.metaDataTable;
	}
	
	/** 
	 * Finds server in a metadata table, that is responsible for given hash value
	 * @param value to be saved on the server
	 * @return server, where given value is between start and end index
	 */
	public KVServerItem findResponsibleServer(byte[] value) {
		for (KVServerItem server: metaDataTable) {
			if (ByteArrayMath.isValueBetweenTwoOthers(value, server.getStartIndex(), server.getEndIndex())) {
				return server;
			}
		}
		return null;
	}

}
