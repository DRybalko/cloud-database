package testing;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.*;

import common.logic.KVServerItem;
import common.logic.MetaDataTableController;

public class MetaDataTableControllerTest {

	MetaDataTableController metaDataTableController;
	
	@Before
	public void initialize(){
		KVServerItem server1 = new KVServerItem("server1", "localhost", "50000");
		KVServerItem server2 = new KVServerItem("server2", "134.123.13.12", "50000");
		metaDataTableController = new MetaDataTableController(Arrays.asList(server1, server2));
	}
	
	@Test
	public void testMetaDataTableInitialized() {
		List<KVServerItem> metaDataTable = metaDataTableController.getMetaDataTable();
		metaDataTableController.initializeTable(2);
		assertTrue(metaDataTable.get(0).getName().equals("server2"));
		assertTrue(metaDataTable.get(0).getIp().equals("134.123.13.12"));
		assertTrue(metaDataTable.get(0).getPort().equals("50000"));
		assertTrue(metaDataTable.get(1).getName().equals("server1"));
		assertTrue(metaDataTable.get(1).getIp().equals("localhost"));
		assertTrue(metaDataTable.get(1).getPort().equals("50000"));
	}
	
	@Test
	public void testAddExistingServerToMetaDataTable() {
		KVServerItem server3 = new KVServerItem("server1", "localhost", "50000");
		server3.setStartIndex(new byte[]{ -41, 23});
		metaDataTableController.initializeTable(2);
		metaDataTableController.addServerToMetaData(server3);
		List<KVServerItem> metaDataTable = metaDataTableController.getMetaDataTable();
		assertTrue(metaDataTable.size() == 2);
		assertTrue(metaDataTable.get(1).getName().equals("server1"));
		assertTrue(metaDataTable.get(1).getIp().equals("localhost"));
		assertTrue(metaDataTable.get(1).getPort().equals("50000"));
		assertTrue(Arrays.equals(metaDataTable.get(1).getStartIndex(), new byte[]{ -41, 23}));
	}
	
}
