package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;

import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.ApbUpdateOperation;

public class AntidoteMapIntegerEntry extends AntidoteMapEntry {
	private int value;
	
	public AntidoteMapIntegerEntry(int value, AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path){
		super(antidoteClient, name, bucket, path);
		this.value = value;
	}
	
	public int getValue(){
		return value;
	}
	
	public void getUpdate(){	
		AntidoteMap outerMap = getClient().readMap(getName(), getBucket());
		AntidoteMapIntegerEntry integer;
		if (getPath().size() == 1){
			integer = outerMap.getIntegerEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
		}
		else{
			AntidoteMapMapEntry innerMap = outerMap.getMapEntry(getPath().get(0).getKey().toStringUtf8());
			for (int i = 1; i<getPath().size()-1; i++){
				innerMap = innerMap.getMapEntry(getPath().get(i).getKey().toStringUtf8());
			}
			integer = innerMap.getIntegerEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
		}		
		value = integer.getValue();
	}
	
	public void increment(){
		increment(1);
	}
	
	public void increment(int inc){
		value = value + inc; //local update
		List<ApbUpdateOperation> integerIncrement = new ArrayList<ApbUpdateOperation>(); 
		integerIncrement.add(getClient().createCounterIncrementOperation(inc));
		ApbUpdateOperation mapUpdate;
		ApbMapKey key;
		List<ApbUpdateOperation> mapUpdateList = new ArrayList<ApbUpdateOperation>();
		for (int i = getPath().size()-1; i>0; i--){
			key = getPath().get(i);
			if (i == getPath().size()-1){
				mapUpdate = getClient().createMapUpdateOperation(key, integerIncrement);
				mapUpdateList.add(mapUpdate);
			}
			else{
				mapUpdate = getClient().createMapUpdateOperation(key, mapUpdateList);
				mapUpdateList = new ArrayList<ApbUpdateOperation>();
				mapUpdateList.add(mapUpdate);
			}
		}
		if (getPath().size()>1){
			getClient().updateMap(getName(), getBucket(), getPath().get(0), mapUpdateList); //update data base
		}
		else if (getPath().size()==1){
			getClient().updateMap(getName(), getBucket(), getPath().get(0), integerIncrement);
		}
	}
	
	public void set(int value){
		this.value += value; //local update
		List<ApbUpdateOperation> integerSet = new ArrayList<ApbUpdateOperation>(); 
		integerSet.add(getClient().createIntegerSetOperation(value));
		ApbUpdateOperation mapUpdate;
		ApbMapKey key;
		List<ApbUpdateOperation> mapUpdateList = new ArrayList<ApbUpdateOperation>();
		for (int i = getPath().size()-1; i>0; i--){
			key = getPath().get(i);
			if (i == getPath().size()-1){
				mapUpdate = getClient().createMapUpdateOperation(key, integerSet);
				mapUpdateList.add(mapUpdate);
			}
			else{
				mapUpdate = getClient().createMapUpdateOperation(key, mapUpdateList);
				mapUpdateList = new ArrayList<ApbUpdateOperation>();
				mapUpdateList.add(mapUpdate);
			}
		}
		if (getPath().size()>1){
			getClient().updateMap(getName(), getBucket(), getPath().get(0), mapUpdateList); //update data base
		}
		else if (getPath().size()==1){
			getClient().updateMap(getName(), getBucket(), getPath().get(0), integerSet);
		}
	}
}
