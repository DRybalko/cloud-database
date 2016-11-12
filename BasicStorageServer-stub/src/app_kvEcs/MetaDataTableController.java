package app_kvEcs;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import org.apache.log4j.Logger;

public class MetaDataTableController {

	private Logger logger;
	private Set<KVServerItem> availableServers;
	private Set<KVServerItem> workingServers;
	private Set<KVServerItem> initializedServers;
	private List<KVServerItem> metaDataTable;
	
	public MetaDataTableController(Set<KVServerItem> availableServers) {
		this.logger = Logger.getRootLogger();
		this.availableServers = availableServers;
		this.metaDataTable = new ArrayList<>();
		this.workingServers = new HashSet<>();
		this.initializedServers = new HashSet<>();
	}
	
	public List<KVServerItem> initializeTable(int numberOfNodes) {
		Iterator<KVServerItem> serversIterator = availableServers.iterator();
		for (int i = 0; i < numberOfNodes; i++) {
			if (serversIterator.hasNext()) {
				KVServerItem server = serversIterator.next();
				byte[] serverEndIndex = generateHashFor(server);
				System.out.println("Server name:" + server.getName() + ", generated index: " + Arrays.toString(serverEndIndex));
				server.setEndIndex(serverEndIndex);
				addServerToMetaData(server);
			} else {
				logger.error("Number of nodes to instantiate is greate then number of servers available in repository. No servers were instanciated");
			}
		}
		return metaDataTable;
	}
	
	private byte[] generateHashFor(KVServerItem server) {
		MessageDigest md = null;
		try {
			 md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			logger.debug("MessageDigest could not be created. "+e.getMessage());
		}	
		byte[] messageToHash = prepareMessageForHash(server.getIp(), server.getPort());
		md.update(messageToHash);
		return md.digest();
	}
	
	public byte[] prepareMessageForHash(String ip, String port) {
		byte[] ipInBytes = ip.getBytes();
		byte[] portInBytes = port.getBytes();
		byte[] mergedMessage = new byte[ipInBytes.length + portInBytes.length];
		System.arraycopy(ipInBytes, 0, mergedMessage, 0, ipInBytes.length);
		System.arraycopy(portInBytes, 0, mergedMessage, ipInBytes.length, portInBytes.length);
		return mergedMessage;
	}
	
	private void addServerToMetaData(KVServerItem server) {
		if (metaDataTable.isEmpty()) addFirstElementToEmptyMetaData(server);
		else if (metaDataTable.size() == 1) addServerToMetaDataTableWithOneElement(server);
		else addServerToMetaDataTableWithMoreThanOneElement(server);
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
			if (isServerBetweenTwoOthers(server, previousNode, nextNode)) {
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
			metaDataTable.set(0, server);
		}
	}
	
	private boolean isServerBetweenTwoOthers(KVServerItem server, KVServerItem node1, KVServerItem node2) {
		return  ByteArrayMath.compareByteArrays(server.getEndIndex(), node1.getEndIndex()) > 0
			&& ByteArrayMath.compareByteArrays(server.getEndIndex(), node2.getEndIndex()) < 0;
	}	

	public void moveFromAvailableToInitialized(KVServerItem server) {
		this.availableServers.remove(server);
		this.initializedServers.add(server);
	}
	
	public void moveFromWorkingToInitialized(KVServerItem server) {
		this.workingServers.remove(server);
		this.initializedServers.add(server);
	}
	
	public void moveFromInitializedToWorking(KVServerItem server) {
		this.initializedServers.remove(server);
		this.workingServers.add(server);
	}
	
	public void moveFromInitializedToAvailable(KVServerItem server) {
		this.initializedServers.remove(server);
		this.availableServers.add(server);
	}
	
	public Set<KVServerItem> getAvailableServers() {
		return this.availableServers;
	}
	
	public Set<KVServerItem> getWorkingServers() {
		return this.workingServers;
	}
	
	public Set<KVServerItem> getInitializedServers() {
		return this.initializedServers;
	}
}
