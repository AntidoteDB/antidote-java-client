package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.ApbUpdateOperation;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

/**
 * The Class AntidoteMapAWMapEntry.
 */
public class AntidoteMapAWMapEntry extends AntidoteMapMapEntry implements AWMapInterface{
	
	/**
	 * Instantiates a new antidote map AW map entry.
	 *
	 * @param entryList the entry list
	 * @param antidoteClient the antidote client
	 * @param name the name
	 * @param bucket the bucket
	 * @param path the path
	 * @param outerMapType the outer map type
	 */
	public AntidoteMapAWMapEntry(List<AntidoteMapEntry> entryList, AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path, CRDT_type outerMapType){
		super(entryList, antidoteClient, name, bucket, path, outerMapType);
	}
	
	/**
	 * Update the entry with the given key.
	 *
	 * @param mapKey the map key
	 * @param update the update
	 */
	public void update(String mapKey, AntidoteMapUpdate update){
		List<AntidoteMapUpdate> updateList = new ArrayList<AntidoteMapUpdate>();
		updateList.add(update);
		update(mapKey, updateList);
	}
	
	/**
	 * Update the entry with the given key with multiple updates.
	 *
	 * @param mapKey the map key
	 * @param updateList the update list
	 */
	public void update(String mapKey, List<AntidoteMapUpdate> updateList){
		updateLocal(mapKey, updateList);
		List<AntidoteMapUpdate> innerMapUpdate = new ArrayList<AntidoteMapUpdate>(); 
		innerMapUpdate.add(getClient().createAWMapUpdate(mapKey, updateList));
		updateHelper(innerMapUpdate);
	}
	
	/**
	 * Removes entries locally.
	 *
	 * @param keyList the key list
	 */
	public void removeLocal(List<ApbMapKey> keyList) {
		List<AntidoteMapEntry> entriesValid = new ArrayList<AntidoteMapEntry>();		
		for (AntidoteMapEntry e : getEntryList()){
			if (! keyList.contains(e.getPath().get(e.getPath().size()-1))){
				entriesValid.add(e);
			}
		}
		setEntryList(entriesValid);
	}


	private void remove(List<String> keyList, CRDT_type type) {
		List<ApbMapKey> apbKeyList = new ArrayList<ApbMapKey>();
		ApbMapKey.Builder apbKeyBuilder = ApbMapKey.newBuilder();
		apbKeyBuilder.setType(type);
		for (String key : keyList){
			apbKeyBuilder.setKey(ByteString.copyFromUtf8(key));
			ApbMapKey apbKey = apbKeyBuilder.build();
			apbKeyList.add(apbKey);
		}
		removeLocal(apbKeyList);
		List<AntidoteMapUpdate> innerMapUpdate = new ArrayList<AntidoteMapUpdate>(); 
		innerMapUpdate.add(getClient().createMapRemove(keyList, type));
		AntidoteMapUpdate mapUpdate;
		List<AntidoteMapUpdate> mapUpdateList = new ArrayList<AntidoteMapUpdate>();
		ApbMapKey key;
		for (int i = getPath().size()-1; i>0; i--){
			key = getPath().get(i);
			if (i == getPath().size()-1){
				mapUpdate = getClient().createAWMapUpdate(key.getKey().toStringUtf8(), innerMapUpdate);
				mapUpdateList.add(mapUpdate);
			}
			else{
				mapUpdate = null; //since we are not at the last position of path, it is one of both kinds of maps
				if(key.getType() == CRDT_type.AWMAP){
					mapUpdate = getClient().createAWMapUpdate(key.getKey().toStringUtf8(), mapUpdateList);
				}
				else if (key.getType() == CRDT_type.GMAP){
					mapUpdate = getClient().createGMapUpdate(key.getKey().toStringUtf8(), mapUpdateList);
				}
				mapUpdateList = new ArrayList<AntidoteMapUpdate>();
				mapUpdateList.add(mapUpdate);
			}
		}
		if (getPath().size()>1){
			List<ApbUpdateOperation> apbMapUpdateList = new ArrayList<ApbUpdateOperation>();
			for (AntidoteMapUpdate u : mapUpdateList){
				apbMapUpdateList.add(u.getOperation());
			}
			if(getOuterMapType() == CRDT_type.AWMAP){
				getClient().updateAWMap(getName(), getBucket(), getPath().get(0), apbMapUpdateList);
			}
			else{
				getClient().updateGMap(getName(), getBucket(), getPath().get(0), apbMapUpdateList);
			}
		}
		else if (getPath().size()==1){
			List<ApbUpdateOperation> apbInnerMapUpdate = new ArrayList<ApbUpdateOperation>();
			for (AntidoteMapUpdate u : innerMapUpdate){
				apbInnerMapUpdate.add(u.getOperation());
			}
			if(getOuterMapType() == CRDT_type.AWMAP){
				getClient().updateAWMap(getName(), getBucket(), getPath().get(0), apbInnerMapUpdate);
			}
			else{
				getClient().updateGMap(getName(), getBucket(), getPath().get(0), apbInnerMapUpdate);
			}
		}		
	}
	
	/**
	 * Removes the counter.
	 *
	 * @param key the key
	 */
	public void removeCounter(String key) {
		List<String> keyList = new ArrayList<String>();
		keyList.add(key);
		remove(keyList, CRDT_type.COUNTER);
	}
	
	/**
	 * Removes the counter.
	 *
	 * @param keyList the key list
	 */
	public void removeCounter(List<String> keyList) {
		remove(keyList, CRDT_type.COUNTER);
	}
	
	/**
	 * Removes the RW set.
	 *
	 * @param key the key
	 */
	public void removeRWSet(String key){
		List<String> keyList = new ArrayList<String>();
		keyList.add(key);
		remove(keyList, CRDT_type.RWSET);
	}
	
	/**
	 * Removes the RW set.
	 *
	 * @param keyList the key list
	 */
	public void removeRWSet(List<String> keyList){
		remove(keyList, CRDT_type.RWSET);
	}
	
	/**
	 * Removes the OR set.
	 *
	 * @param key the key
	 */
	public void removeORSet(String key){
		List<String> keyList = new ArrayList<String>();
		keyList.add(key);
		remove(keyList, CRDT_type.ORSET);
	}
	
	/**
	 * Removes the OR set.
	 *
	 * @param keyList the key list
	 */
	public void removeORSet(List<String> keyList){
		remove(keyList, CRDT_type.ORSET);
	}
	
	/**
	 * Removes the register.
	 *
	 * @param key the key
	 */
	public void removeRegister(String key) {
		List<String> keyList = new ArrayList<String>();
		keyList.add(key);
		remove(keyList, CRDT_type.LWWREG);
	}
	
	/**
	 * Removes the register.
	 *
	 * @param keyList the key list
	 */
	public void removeRegister(List<String> keyList) {
		remove(keyList, CRDT_type.LWWREG);
	}
	
	/**
	 * Removes the MV register.
	 *
	 * @param key the key
	 */
	public void removeMVRegister(String key) {
		List<String> keyList = new ArrayList<String>();
		keyList.add(key);
		remove(keyList, CRDT_type.MVREG);
	}
	
	/**
	 * Removes the MV register.
	 *
	 * @param keyList the key list
	 */
	public void removeMVRegister(List<String> keyList) {
		remove(keyList, CRDT_type.MVREG);
	}
	
	/**
	 * Removes the integer.
	 *
	 * @param key the key
	 */
	public void removeInteger(String key) {
		List<String> keyList = new ArrayList<String>();
		keyList.add(key);
		remove(keyList, CRDT_type.INTEGER);
	}
	
	/**
	 * Removes the integer.
	 *
	 * @param keyList the key list
	 */
	public void removeInteger(List<String> keyList) {
		remove(keyList, CRDT_type.INTEGER);
	}
	
	/**
	 * Removes the AW map.
	 *
	 * @param key the key
	 */
	public void removeAWMap(String key) {
		List<String> keyList = new ArrayList<String>();
		keyList.add(key);
		remove(keyList, CRDT_type.AWMAP);
	}
	
	/**
	 * Removes the AW map.
	 *
	 * @param keyList the key list
	 */
	public void removeAWMap(List<String> keyList){
		remove(keyList, CRDT_type.AWMAP);
	}
	
	/**
	 * Removes the G map.
	 *
	 * @param key the key
	 */
	public void removeGMap(String key) {
		List<String> keyList = new ArrayList<String>();
		keyList.add(key);
		remove(keyList, CRDT_type.GMAP);
	}
	
	/**
	 * Removes the G map.
	 *
	 * @param keyList the key list
	 */
	public void removeGMap(List<String> keyList){
		remove(keyList, CRDT_type.GMAP);
	}
}