package main.java.AntidoteClient;

public class AntidoteRegister extends AntidoteObject {
	private String value;
	private AntidoteClient antidoteClient;
	
	public AntidoteRegister(String name, String bucket, String value, AntidoteClient antidoteClient) {
		super(name, bucket);
		this.value = value;
		this.antidoteClient = antidoteClient;
	}
	
	public String getValue(){
		return value;
	}
	
	public void getUpdate(){
		value = antidoteClient.readRegister(getName(), getBucket()).getValue();
	}
	
	public void update(String element){
		value = element;
		antidoteClient.updateRegister(getName(), getBucket(), element);
	}
}
