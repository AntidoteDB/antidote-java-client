package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;

import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.ApbUpdateOperation;

public class AntidoteMapMVRegisterEntry extends AntidoteMapEntry {
private List<String> valueList;
	
	public AntidoteMapMVRegisterEntry(List<String> valueList, AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path){
		super(antidoteClient, name, bucket, path);
		this.valueList = valueList;
	}
	
	public void getUpdate(){	
		AntidoteMap outerMap = getClient().readMap(getName(), getBucket());
		AntidoteMapMVRegisterEntry register;
		if (getPath().size() == 1){
			register = outerMap.getMVRegisterEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
		}
		else{
			AntidoteMapMapEntry innerMap = outerMap.getMapEntry(getPath().get(0).getKey().toStringUtf8());
			for (int i = 1; i<getPath().size()-1; i++){
				innerMap = innerMap.getMapEntry(getPath().get(i).getKey().toStringUtf8());
			}
			register = innerMap.getMVRegisterEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
		}		
		valueList = register.getValueList();
	}
	
	public List<String> getValueList(){
		return valueList;
	}
	
	public void update(String value){
		List<ApbUpdateOperation> registerSet = new ArrayList<ApbUpdateOperation>(); 
		registerSet.add(getClient().createRegisterSetOperation(value));
		ApbUpdateOperation mapUpdate;
		ApbMapKey key;
		List<ApbUpdateOperation> mapUpdateList = new ArrayList<ApbUpdateOperation>();
		for (int i = getPath().size()-1; i>0; i--){
			key = getPath().get(i);
			if (i == getPath().size()-1){
				mapUpdate = getClient().createMapUpdateOperation(key, registerSet);
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
			getClient().updateMap(getName(), getBucket(), getPath().get(0), registerSet);
		}
		getUpdate();
	}
}
