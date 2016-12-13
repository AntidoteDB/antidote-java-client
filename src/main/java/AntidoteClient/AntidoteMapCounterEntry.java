package main.java.AntidoteClient;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.ApbUpdateOperation;
import java.util.ArrayList;
import java.util.List;

public class AntidoteMapCounterEntry extends AntidoteMapEntry {
	private int value;
	
	public AntidoteMapCounterEntry(int value, AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path){
		super(antidoteClient, name, bucket, path);
		this.value = value;
	}
	
	public void getUpdate(){	
		AntidoteMap outerMap = getClient().readMap(getName(), getBucket());
		AntidoteMapCounterEntry counter;
		if (getPath().size() == 1){
			counter = outerMap.getCounterEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
		}
		else{
			AntidoteMapMapEntry innerMap = outerMap.getMapEntry(getPath().get(0).getKey().toStringUtf8());
			for (int i = 1; i<getPath().size()-1; i++){
				innerMap = innerMap.getMapEntry(getPath().get(i).getKey().toStringUtf8());
			}
			counter = innerMap.getCounterEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
		}		
		value = counter.getValue();
	}
	
	public int getValue(){
		return value;
	}
	
	public void increment(){
		increment(1);
	}
	
	public void increment(int inc){
		value = value + inc; //update local AntidoteCounter object
		List<ApbUpdateOperation> counterIncrement = new ArrayList<ApbUpdateOperation>(); 
		counterIncrement.add(getClient().createCounterIncrementOperation(inc));
		ApbUpdateOperation mapUpdate;
		ApbMapKey key;
		List<ApbUpdateOperation> mapUpdateList = new ArrayList<ApbUpdateOperation>();
		for (int i = getPath().size()-1; i>0; i--){
			key = getPath().get(i);
			if (i == getPath().size()-1){
				mapUpdate = getClient().createMapUpdateOperation(key, counterIncrement);
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
			getClient().updateMap(getName(), getBucket(), getPath().get(0), counterIncrement);
		}
	}
}
