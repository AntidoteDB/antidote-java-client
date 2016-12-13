package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;

import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.ApbUpdateOperation;

public class AntidoteMapSetEntry extends AntidoteMapEntry {
	private List<String> valueList;
	public AntidoteMapSetEntry(List<String> valueList, AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path){
		super(antidoteClient, name, bucket, path);
		this.valueList = valueList;
	}
	
	public void getUpdate(){	
		AntidoteMap outerMap = getClient().readMap(getName(), getBucket());
		AntidoteMapSetEntry set;
		if (getPath().size() == 1){
			set = outerMap.getSetEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
		}
		else{
			AntidoteMapMapEntry innerMap = outerMap.getMapEntry(getPath().get(0).getKey().toStringUtf8());
			for (int i = 1; i<getPath().size()-1; i++){
				innerMap = innerMap.getMapEntry(getPath().get(i).getKey().toStringUtf8());
			}
			set = innerMap.getSetEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
		}		
		valueList = set.getValueList();
	}
	
	public List<String> getValueList(){
		return valueList;
	}
	
	public void addElement(String element){
		List<String> elementList = new ArrayList<String>();
		elementList.add(element);
		addElement(elementList);
	}
	
	public void addElement(List<String> elementList){
		for (String e : elementList){
			if (! valueList.contains(e)){
				valueList.add(e);
			}
		} // local update
		List<ApbUpdateOperation> setAdd = new ArrayList<ApbUpdateOperation>(); 
		setAdd.add(getClient().createSetAddElementOperation(elementList));
		ApbUpdateOperation mapUpdate;
		ApbMapKey key;
		List<ApbUpdateOperation> mapUpdateList = new ArrayList<ApbUpdateOperation>();
		for (int i = getPath().size()-1; i>0; i--){
			key = getPath().get(i);
			if (i == getPath().size()-1){
				mapUpdate = getClient().createMapUpdateOperation(key, setAdd);
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
			getClient().updateMap(getName(), getBucket(), getPath().get(0), setAdd);
		}
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
		List<ApbUpdateOperation> setRemove = new ArrayList<ApbUpdateOperation>(); 
		setRemove.add(getClient().createSetRemoveElementOperation(elementList));
		ApbUpdateOperation mapUpdate;
		ApbMapKey key;
		List<ApbUpdateOperation> mapUpdateList = new ArrayList<ApbUpdateOperation>();
		for (int i = getPath().size()-1; i>0; i--){
			key = getPath().get(i);
			if (i == getPath().size()-1){
				mapUpdate = getClient().createMapUpdateOperation(key, setRemove);
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
			getClient().updateMap(getName(), getBucket(), getPath().get(0), setRemove);
		}
	}
	
}
