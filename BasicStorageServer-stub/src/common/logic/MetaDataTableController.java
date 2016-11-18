package common.logic;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import org.apache.log4j.Logger;

public class MetaDataTableController {

	private Logger logger;
	private List<KVServerItem> metaDataTable;
	private List<KVServerItem> availableServers;
	
	public MetaDataTableController(List<KVServerItem> availableServers) {
		this.logger = Logger.getRootLogger();
		this.metaDataTable = new LinkedList<>();
		this.availableServers = availableServers;
	}
	
	public List<KVServerItem> initializeTable(int numberOfNodes) {
		Iterator<KVServerItem> serversIterator = availableServers.iterator();
		for (int i = 0; i < numberOfNodes; i++) {
			if (serversIterator.hasNext()) {
				KVServerItem server = serversIterator.next();
				byte[] serverEndIndex = HashGenerator.generateHashForValues(server.getIp(), server.getPort());
				server.setEndIndex(serverEndIndex);
				addServerToMetaData(server);
			} else {
				logger.error("Number of nodes to instantiate is greate then number of servers available in repository. No servers were instanciated");
			}
		}
		return metaDataTable;
	}
	
	public KVServerItem addServerToMetaData(KVServerItem server) {
		if (metaDataTable.isEmpty()) addFirstElementToEmptyMetaData(server);
		else if (metaDataTable.size() == 1) addServerToMetaDataTableWithOneElement(server);
		else addServerToMetaDataTableWithMoreThanOneElement(server);
		return server;
	}
	
	private void addFirstElementToEmptyMetaData(KVServerItem server) {
		byte[] serverStartIndex = ByteArrayMath.increment(server.getEndIndex());
		server.setStartIndex(serverStartIndex);
		metaDataTable.add(server);
	}
	
	private void addServerToMetaDataTableWithOneElement(KVServerItem server) {
		KVServerItem existingNode = metaDataTable.get(0);
		if (ByteArrayMath.compareByteArrays(server.getEndIndex(), existingNode.getStartIndex()) < 0) {
			insertAtPosition(server, existingNode, 0);
		} else {
			insertAtPosition(server, existingNode, 1);
		}
	}
	
	private void insertAtPosition(KVServerItem serverToInsert, KVServerItem existingNode, int position) {
		serverToInsert.setStartIndex(ByteArrayMath.increment(existingNode.getEndIndex()));
		existingNode.setStartIndex(ByteArrayMath.increment(serverToInsert.getEndIndex()));
		metaDataTable.add(position, serverToInsert);
	}

	private void addServerToMetaDataTableWithMoreThanOneElement(KVServerItem server) {
		ListIterator<KVServerItem> iterator = metaDataTable.listIterator();
		KVServerItem previousNode = iterator.next();
		KVServerItem nextNode;
		while (iterator.hasNext()) {
			nextNode = iterator.next();
			if (isValueBetweenTwoOthers(server.getEndIndex(), previousNode.getEndIndex(), nextNode.getEndIndex())) {
				server.setStartIndex(ByteArrayMath.increment(previousNode.getEndIndex()));
				nextNode.setStartIndex(ByteArrayMath.increment(server.getEndIndex()));
				metaDataTable.add(iterator.previousIndex(), server);
				return;
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
	}
	
	public boolean isValueBetweenTwoOthers(byte[] newValue, byte[] node1, byte[] node2) {
		return  ByteArrayMath.compareByteArrays(newValue, node1) > 0
			&& ByteArrayMath.compareByteArrays(newValue, node2) < 0;
	}	
	
	public List<KVServerItem> getMetaDataTable() {
		return this.metaDataTable;
	}
	
	public KVServerItem findResponsibleServer(byte[] value) {
		for (KVServerItem server: metaDataTable) {
			if (isValueBetweenTwoOthers(value, server.getStartIndex(), server.getEndIndex())) {
				return server;
			}
		}
		return null;
	}
}
