package app_kvServer;

public class Range {

	private String from;
	private String to;
	private static final byte SEPARATOR = 45;
	
	
	public Range (String from, String to){
		this.from = from;
		this.to = to;
	}
		
	public Range(byte[] bytes){
		int i=0;		        
        while(bytes[i] != SEPARATOR){
        	i++;
        }       
        byte [] from = new byte[i];
        byte [] to = new byte[bytes.length - i-1];
               
        for(int j = 0; j<= i-1; j++){
        	from[j] = bytes[j];
        }
        
        this.from = new String(from);
		
		for(int j = 0; j<bytes.length - i-1; j++){
			to[j] = bytes[j+i+1];
		}
        this.to = new String(to);	
	}
	

	public String getLower_limit() {
		return from;
	}
	
	public void setFrom(String from){
		this.from = from;
	}
	public String getFrom(){
		return from;
	}
	
	public void setTo(String to){
		this.to = to;
	}
	public String getTo(){
		return to;
	}

}
