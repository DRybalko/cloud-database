package app_kvServer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * 
 * This class provides the storage logic that persists data
 * to disk (e.g., a file). It provides the functions to delete,
 * store, update, find etc. a tuple from/to the disk.
 * 
 *  @see Properties
 *
 */
public class StorageCommunicator {
	
	private final String FILE_PATH = "storage/storage.txt";
	
	private Logger logger;
	
	private File file; 		
	private Properties storage;
	
	public StorageCommunicator() {
		logger = Logger.getRootLogger();
		initialize();
	}
	
	private void initialize() {
		file = new File(FILE_PATH);
		storage = new Properties();	
	}
	
	public void put(String key, String value) {
		storage.put(key, value);
		writeToFile();
		
	}
	
	private void readFromFile(){
		try {
			storage.load(new FileInputStream(file));
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}
	
	private void writeToFile(){
		OutputStream outputStream;
		try {
			outputStream = new FileOutputStream(file);
			storage.store(outputStream, null);
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}
	
	public boolean deleteFromStorage(String key) {
		readFromFile();
		if (!storage.containsKey(key)) return false;
		storage.remove(key);
		writeToFile();
		return true;
	}
		
	public String readValueFor(String key) {
		readFromFile();
		return storage.getProperty(key);
	}
}
