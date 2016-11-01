package app_kvServer.cache;

import app_kvServer.KVTuple;

public interface CacheStrategy {
	
	public boolean contains(String key);
	
	public void addElement(String key, String value);
	
	public String getValueFor(String key);

	public void updateElement(String key, String value);
	
	public KVTuple deleteElement();
	
	public int size();
	
	public void deleteValueFor(String key);
	
}
