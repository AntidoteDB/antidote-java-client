package main.java.AntidoteClient;

public class AntidoteInteger extends AntidoteObject {
	private int value;
	private AntidoteClient antidoteClient;
	
	public AntidoteInteger(String name, String bucket, int value, AntidoteClient antidoteClient) {
		super(name, bucket);
		this.value = value;
		this.antidoteClient = antidoteClient;
	}
	
	public int getValue(){
		return value;
	}
	
	public void setValue(int newValue){
		value = newValue;
		antidoteClient.setInteger(getName(), getBucket(), value);
	}
	
	public void getUpdate(){
		value = antidoteClient.readInteger(getName(), getBucket()).getValue();
	}
	
	public void increment(){
		increment(1);
	}
	
	public void increment(int inc){
		value = value + inc; //update local AntidoteCounter object
		antidoteClient.incrementInteger(getName(), getBucket(), inc); //update data base
	}
}
