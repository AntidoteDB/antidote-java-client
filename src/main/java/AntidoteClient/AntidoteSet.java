package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;

public class AntidoteSet extends AntidoteObject {
	private List<String> valueList;
	private AntidoteClient antidoteClient;
	
	public AntidoteSet(String name, String bucket, List<String> valueList, AntidoteClient antidoteClient) {
		super(name, bucket);
		this.valueList = valueList;
		this.antidoteClient = antidoteClient;
	}
	
	public List<String> getValueList(){
		return valueList;
	}
	
	public void getUpdate(){
		valueList = antidoteClient.readSet(getName(), getBucket()).getValueList();
	}
	
	public void add(String element){
		List<String> elementList = new ArrayList<String>();
		elementList.add(element);
		add(elementList);
	}
	
	public void add(List<String> elementList){
		for (String e : elementList){
			if (! valueList.contains(e)){
				valueList.add(e);
			}
		}
		antidoteClient.addSetElement(getName(), getBucket(), elementList);
	}
	
	public void remove(String element){
		List<String> elementList = new ArrayList<String>();
		elementList.add(element);
		remove(elementList);
	}
	
	public void remove(List<String> elementList){
		for (String e : elementList){
			if (valueList.contains(e)){
				valueList.remove(e);
			}
		}
		antidoteClient.removeSetElement(getName(), getBucket(), elementList);
	}
}
