package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;

import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.ApbUpdateOperation;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

public class AntidoteMapMapEntry extends AntidoteMapEntry {
	private List<AntidoteMapEntry> entryList;
	
	public AntidoteMapMapEntry(List<AntidoteMapEntry> entryList, AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path){
		super(antidoteClient, name, bucket, path);
		this.entryList = entryList;
	}
	
	public List<AntidoteMapEntry> getEntryList(){
		return entryList;
	}
	
	public void getUpdate(){	
		AntidoteMap outerMap = getClient().readMap(getName(), getBucket());
		AntidoteMapMapEntry innerMap = outerMap.getMapEntry(getPath().get(0).getKey().toStringUtf8());
		for (int i = 1; i<getPath().size(); i++){
			innerMap = innerMap.getMapEntry(getPath().get(i).getKey().toStringUtf8());
		}
		entryList = innerMap.getEntryList();
	}

	//TODO: migrate to AntidoteClient, also in AntidoteMap
	public AntidoteMapUpdate createCounterIncrement(){
		return createCounterIncrement(1);
	}
	
	public AntidoteMapUpdate createCounterIncrement(int inc){
		return new AntidoteMapUpdate(CRDT_type.COUNTER, getClient().createCounterIncrementOperation(inc));
	}
	
	public AntidoteMapUpdate createIntegerIncrement(){
		return createCounterIncrement(1);
	}
	
	public AntidoteMapUpdate createIntegerIncrement(int inc){
		return new AntidoteMapUpdate(CRDT_type.INTEGER, getClient().createIntegerIncrementOperation(inc));
	}
	
	public AntidoteMapUpdate createIntegerSet(int value){
		return new AntidoteMapUpdate(CRDT_type.INTEGER, getClient().createIntegerSetOperation(value));
	}
	
	public AntidoteMapUpdate createSetAdd(String element){
		List<String> elementList = new ArrayList<String>();
		elementList.add(element);
		return createSetAdd(elementList);
	}
	
	public AntidoteMapUpdate createSetAdd(List<String> elementList){
		return new AntidoteMapUpdate(CRDT_type.ORSET, getClient().createSetAddElementOperation(elementList));
	}
	
	public AntidoteMapUpdate createSetRemove(String element){
		List<String> elementList = new ArrayList<String>();
		elementList.add(element);
		return createSetRemove(elementList);
	}
	
	public AntidoteMapUpdate createSetRemove(List<String> elementList){
		return new AntidoteMapUpdate(CRDT_type.ORSET, getClient().createSetRemoveElementOperation(elementList));
	}
	
	public AntidoteMapUpdate createRegisterSet(String value){
		return new AntidoteMapUpdate(CRDT_type.LWWREG, getClient().createRegisterSetOperation(value));
	}
	
	public AntidoteMapUpdate createMVRegisterSet(String value){
		return new AntidoteMapUpdate(CRDT_type.MVREG, getClient().createRegisterSetOperation(value));
	}
	
	public AntidoteMapUpdate createMapUpdate(String key, AntidoteMapUpdate update) throws Exception{
		List<AntidoteMapUpdate> updateList = new ArrayList<AntidoteMapUpdate>();
		updateList.add(update);
		return createMapUpdate(key, updateList);
	}
	
	public AntidoteMapUpdate createMapUpdate(String key, List<AntidoteMapUpdate> updateList) throws Exception {
		CRDT_type type = updateList.get(0).getType();
		List<ApbUpdateOperation> apbUpdateList = new ArrayList<ApbUpdateOperation>();
		for (AntidoteMapUpdate u : updateList){
			if (!(type.equals(u.getType()))){
				throw new Exception("Different types detected, only one type allowed");
			}
			apbUpdateList.add(u.getOperation());
		}
		ApbMapKey.Builder apbKeyBuilder = ApbMapKey.newBuilder();
		apbKeyBuilder.setKey(ByteString.copyFromUtf8(key));
		apbKeyBuilder.setType(type);
		ApbMapKey apbKey = apbKeyBuilder.build();
		return new AntidoteMapUpdate(CRDT_type.AWMAP, getClient().createMapUpdateOperation(apbKey, apbUpdateList));
	}
	//TODO: Take a look at these methods and whether they would be useful 
	public AntidoteMapUpdate createMapCounterRemove(String key) throws Exception {
		List<String> keyList = new ArrayList<String>();
		keyList.add(key);
		return createMapCounterRemove(keyList);
	}
	
	public AntidoteMapUpdate createMapCounterRemove(List<String> keyList) throws Exception {
		List<ApbMapKey> apbKeyList = new ArrayList<ApbMapKey>();
		ApbMapKey.Builder keyBuilder = ApbMapKey.newBuilder();
		keyBuilder.setType(CRDT_type.COUNTER);
		for (String key : keyList){
			keyBuilder.setKey(ByteString.copyFromUtf8(key));
			apbKeyList.add(keyBuilder.build());
		}
		return new AntidoteMapUpdate(CRDT_type.AWMAP, getClient().createMapRemoveOperation(apbKeyList));
	}
	
	public void update(String mapKey, AntidoteMapUpdate update) throws Exception{
		List<AntidoteMapUpdate> updateList = new ArrayList<AntidoteMapUpdate>();
		updateList.add(update);
		update(mapKey, updateList);
	}
	
	public void update(String mapKey, List<AntidoteMapUpdate> updateList) throws Exception{
		ApbMapKey.Builder apbKeyBuilder = ApbMapKey.newBuilder();
		CRDT_type type = updateList.get(0).getType();
		for (AntidoteMapUpdate u : updateList){
			if (!(type.equals(u.getType()))){
				throw new Exception("Different types detected, only one type allowed");
			}
		}
		apbKeyBuilder.setType(type);
		apbKeyBuilder.setKey(ByteString.copyFromUtf8(mapKey));
		ApbMapKey apbKey = apbKeyBuilder.build();
		List<ApbUpdateOperation> apbUpdateList = new ArrayList<ApbUpdateOperation>();
		for (AntidoteMapUpdate u:updateList){
			apbUpdateList.add(u.getOperation());
			
		}	
		List<ApbUpdateOperation> innerMapUpdate = new ArrayList<ApbUpdateOperation>(); 
		innerMapUpdate.add(getClient().createMapUpdateOperation(apbKey, apbUpdateList));
		ApbUpdateOperation mapUpdate;
		List<ApbUpdateOperation> mapUpdateList = new ArrayList<ApbUpdateOperation>();
		ApbMapKey key;
		for (int i = getPath().size()-1; i>0; i--){
			key = getPath().get(i);
			if (i == getPath().size()-1){
				mapUpdate = getClient().createMapUpdateOperation(key, innerMapUpdate);
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
			getClient().updateMap(getName(), getBucket(), getPath().get(0), innerMapUpdate);
		}
		getUpdate();
	}

	private void remove(List<ApbMapKey> keyList){
		List<AntidoteMapEntry> entriesValid = new ArrayList<AntidoteMapEntry>();		
		for (AntidoteMapEntry e : entryList){
			if (! keyList.contains(e.getPath().get(e.getPath().size()-1))){
				entriesValid.add(e);
			}
		}
		entryList = entriesValid; //local changes done
		List<ApbUpdateOperation> innerMapUpdate = new ArrayList<ApbUpdateOperation>(); 
		innerMapUpdate.add(getClient().createMapRemoveOperation(keyList));
		ApbUpdateOperation mapUpdate;
		List<ApbUpdateOperation> mapUpdateList = new ArrayList<ApbUpdateOperation>();
		ApbMapKey key;
		for (int i = getPath().size()-1; i>0; i--){
			key = getPath().get(i);
			if (i == getPath().size()-1){
				mapUpdate = getClient().createMapUpdateOperation(key, innerMapUpdate);
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
			getClient().updateMap(getName(), getBucket(), getPath().get(0), innerMapUpdate);
		}		
	}
	
	public void removeCounter(String key){
		List<String> keyList = new ArrayList<String>();
		keyList.add(key);
		removeCounter(keyList);
	}
	
	public void removeCounter(List<String> keyList){
		List<ApbMapKey> apbKeyList = new ArrayList<ApbMapKey>();
		ApbMapKey.Builder apbKeyBuilder = ApbMapKey.newBuilder();
		apbKeyBuilder.setType(CRDT_type.COUNTER);
		for (String key : keyList){
			apbKeyBuilder.setKey(ByteString.copyFromUtf8(key));
			ApbMapKey apbKey = apbKeyBuilder.build();
			apbKeyList.add(apbKey);
		}
		remove(apbKeyList);
	}
	
	public void removeSet(String key){
		List<String> keyList = new ArrayList<String>();
		keyList.add(key);
		removeSet(keyList);
	}
	
	public void removeSet(List<String> keyList){
		List<ApbMapKey> apbKeyList = new ArrayList<ApbMapKey>();
		ApbMapKey.Builder apbKeyBuilder = ApbMapKey.newBuilder();
		apbKeyBuilder.setType(CRDT_type.ORSET);
		for (String key : keyList){
			apbKeyBuilder.setKey(ByteString.copyFromUtf8(key));
			ApbMapKey apbKey = apbKeyBuilder.build();
			apbKeyList.add(apbKey);
		}
		remove(apbKeyList);
	}
	
	public void removeInteger(String key){
		List<String> keyList = new ArrayList<String>();
		keyList.add(key);
		removeInteger(keyList);
	}
	
	public void removeInteger(List<String> keyList){
		List<ApbMapKey> apbKeyList = new ArrayList<ApbMapKey>();
		ApbMapKey.Builder apbKeyBuilder = ApbMapKey.newBuilder();
		apbKeyBuilder.setType(CRDT_type.INTEGER);
		for (String key : keyList){
			apbKeyBuilder.setKey(ByteString.copyFromUtf8(key));
			ApbMapKey apbKey = apbKeyBuilder.build();
			apbKeyList.add(apbKey);
		}
		remove(apbKeyList);
	}
	
	public void removeMap(String key){
		List<String> keyList = new ArrayList<String>();
		keyList.add(key);
		removeMap(keyList);
	}
	
	public void removeMap(List<String> keyList){
		List<ApbMapKey> apbKeyList = new ArrayList<ApbMapKey>();
		ApbMapKey.Builder apbKeyBuilder = ApbMapKey.newBuilder();
		apbKeyBuilder.setType(CRDT_type.AWMAP);
		for (String key : keyList){
			apbKeyBuilder.setKey(ByteString.copyFromUtf8(key));
			ApbMapKey apbKey = apbKeyBuilder.build();
			apbKeyList.add(apbKey);
		}
		remove(apbKeyList);
	}
	
	public AntidoteMapCounterEntry getCounterEntry(String key){
		AntidoteMapCounterEntry counter = null;
		for (AntidoteMapEntry e : entryList){
			if (e.getPath().get(e.getPath().size()-1).getType().equals(CRDT_type.COUNTER) && e.getPath().get(e.getPath().size()-1).getKey().toStringUtf8().equals(key)){
				counter = (AntidoteMapCounterEntry)e;
			}
		}
		return counter;
	}
	
	public AntidoteMapSetEntry getSetEntry(String key){
		AntidoteMapSetEntry set = null;
		for (AntidoteMapEntry e : entryList){
			if (e.getPath().get(e.getPath().size()-1).getType().equals(CRDT_type.ORSET) && e.getPath().get(e.getPath().size()-1).getKey().toStringUtf8().equals(key)){
				set = (AntidoteMapSetEntry) e;
			}
		}
		return set;
	}
	
	public AntidoteMapRegisterEntry getRegisterEntry(String key){
		AntidoteMapRegisterEntry register = null;
		for (AntidoteMapEntry e : entryList){
			if (e.getPath().get(e.getPath().size()-1).getType().equals(CRDT_type.LWWREG) && e.getPath().get(e.getPath().size()-1).getKey().toStringUtf8().equals(key)){
				register = (AntidoteMapRegisterEntry) e;
			};
		}
		return register;
	}
	
	public AntidoteMapMVRegisterEntry getMVRegisterEntry(String key){
		AntidoteMapMVRegisterEntry register = null;
		for (AntidoteMapEntry e : entryList){
			if (e.getPath().get(e.getPath().size()-1).getType().equals(CRDT_type.MVREG) && e.getPath().get(e.getPath().size()-1).getKey().toStringUtf8().equals(key)){
				register = (AntidoteMapMVRegisterEntry) e;
			};
		}
		return register;
	}
	
	public AntidoteMapIntegerEntry getIntegerEntry(String key){
		AntidoteMapIntegerEntry integer = null;
		for (AntidoteMapEntry e : entryList){
			if (e.getPath().get(e.getPath().size()-1).getType().equals(CRDT_type.INTEGER) && e.getPath().get(e.getPath().size()-1).getKey().toStringUtf8().equals(key)){
				integer = (AntidoteMapIntegerEntry) e;
			}
		}
		return integer;
	}
	
	public AntidoteMapMapEntry getMapEntry(String key){
		AntidoteMapMapEntry map = null;
		for (AntidoteMapEntry e : entryList){
			if (e.getPath().get(e.getPath().size()-1).getType().equals(CRDT_type.AWMAP) && e.getPath().get(e.getPath().size()-1).getKey().toStringUtf8().equals(key)){
				map = (AntidoteMapMapEntry) e;
			}
		}
		return map;
	}
}