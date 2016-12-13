package main.java.AntidoteClient;

import java.util.List;

public class AntidoteMVRegister extends AntidoteObject {
	private List<String> valueList;
	private AntidoteClient antidoteClient;
	
	public AntidoteMVRegister(String name, String bucket, List<String> valueList, AntidoteClient antidoteClient) {
		super(name, bucket);
		this.valueList = valueList;
		this.antidoteClient = antidoteClient;
	}
	
	public List<String> getValueList(){
		return valueList;
	}
	
	public void getUpdate(){
		valueList = antidoteClient.readMVRegister(getName(), getBucket()).getValueList();
	}
	
	public void update(String element){
		antidoteClient.updateMVRegister(getName(), getBucket(), element);
		valueList = antidoteClient.readMVRegister(getName(), getBucket()).getValueList(); //Lazy solution, but I have no clue, how MV registers work here
	}
}
