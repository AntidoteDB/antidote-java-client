package main.java.AntidoteClient;

public class AntidoteCounter extends AntidoteObject {
	private int value;
	private AntidoteClient antidoteClient;
	
	public AntidoteCounter(String name, String bucket, int value, AntidoteClient antidoteClient) {
		super(name, bucket);
		this.value = value;
		this.antidoteClient = antidoteClient;
	}
	
	public int getValue(){
		return value;
	}
	
	public void getUpdate(){
		value = antidoteClient.readCounter(getName(), getBucket()).getValue();
	}
	
	public void increment(){
		increment(1);
	}
	
	public void increment(int inc){
		value = value + inc; //update local AntidoteCounter object
		antidoteClient.updateCounter(getName(), getBucket(), inc); //update data base
	}
}
