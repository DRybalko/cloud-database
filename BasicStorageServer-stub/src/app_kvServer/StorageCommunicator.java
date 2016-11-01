package app_kvServer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

public class StorageCommunicator {

	
	
	
	private static final String FILE_PATH = "storage/storage.txt";
	private final static String SEPARATOR = "=";
	
	private static FileInputStream inputStream;
	private static FileOutputStream outputStream;
	private static Logger logger;
	
	private static File file; 		
	private static Properties storage;
	
	public StorageCommunicator() {
		logger = Logger.getRootLogger();
		initialize();
	}
	
	private void initialize() {
		file = new File(FILE_PATH);
		storage = new Properties();	
	}
	
	public static void put(String key, String value) {
		storage.put(key, value);
		writeToFile();
		
	}
	private static void readFromFile(){
		try {
			storage.load(new FileInputStream(file));
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}
	private static void writeToFile(){
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
	
	public static void deleteFromStorage(String key) throws IOException{
		readFromFile();
		storage.remove(key);
		writeToFile();
	}
	
	
	public static String readValueFor(String key) {
		readFromFile();
		return storage.getProperty(key);
	}
	
	
	public static void main(String [] args) throws IOException{
		StorageCommunicator sc = new StorageCommunicator();
		put("10", "This");
		put("11", "is");
		put("12", "a");
		put("13", "test");
		deleteFromStorage("11");
		deleteFromStorage("12");
		System.out.println(readValueFor("10"));
		put("10", "update");
		System.out.println(readValueFor("10"));
		
	}
}
