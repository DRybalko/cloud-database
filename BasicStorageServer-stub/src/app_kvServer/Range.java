package app_kvServer;

public class Range {

	private String from;
	private String to;
	
	
	public Range (String from, String to){
		this.from = from;
		this.to = to;
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
