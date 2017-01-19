package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.ApbUpdateOperation;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

/**
 * The Class AntidoteAWMap.
 */
public class AntidoteAWMap extends AntidoteMap implements AWMapInterface{
	
	/**
	 * Instantiates a new antidote AW map.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param entryList the map's entries
	 * @param antidoteClient the antidote client
	 */
	public AntidoteAWMap(String name, String bucket, List<AntidoteMapEntry> entryList, AntidoteClient antidoteClient) {
		super(name, bucket, entryList, antidoteClient);
	}
	
	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){
		if (getUpdateList().size() > 0){
			throw new AntidoteException("You can't read the database without pushing your changes first or rolling back");
		}
		setEntryList(getClient().readAWMap(getName(), getBucket()).getEntryList());
	}
	
	public void rollBack(){
		clearUpdateList();
		readDatabase();
	}
	
	public void synchronize(){
		push();
		readDatabase();
	}

	/**
	 * Execute updates locally.
	 *
	 * @param key the key of the entry which is updated
	 * @param updateList updates which are executed on that entry
	 */
	public void updateLocal(String key, List<AntidoteMapUpdate> updateList){
		super.updateLocal(key, updateList, CRDT_type.AWMAP);	
	}
	
	/**
	 * Update.
	 *
	 * @param key the key of the entry which is updated
	 * @param update the update which is executed on that entry
	 */
	public void update(String key, AntidoteMapUpdate update){
		List<AntidoteMapUpdate> updateList = new ArrayList<AntidoteMapUpdate>();
		updateList.add(update);
		update(key, updateList);
	}
	
	/**
	 * Update.
	 *
	 * @param key the key of the entry which is updated
	 * @param updateList updates which are executed on that entry
	 */
	public void update(String key, List<AntidoteMapUpdate> updateList){
		ApbMapKey.Builder apbKeyBuilder = ApbMapKey.newBuilder();
		CRDT_type type = updateList.get(0).getType();
		for (AntidoteMapUpdate u : updateList){
			if (!(type.equals(u.getType()))){
				throw new  IllegalArgumentException("Different types detected, only one type allowed");
			}
		}
		apbKeyBuilder.setType(type);
		apbKeyBuilder.setKey(ByteString.copyFromUtf8(key));
		ApbMapKey apbKey = apbKeyBuilder.build();
		List<ApbUpdateOperation> apbUpdateList = new ArrayList<ApbUpdateOperation>();
		for (AntidoteMapUpdate u : updateList){
			apbUpdateList.add(u.getOperation());
		}	
		addUpdateToList(apbKey, apbUpdateList);
		updateLocal(key, updateList);		
	}
	
	/**
	 * Removes entries locally.
	 *
	 * @param keyList the key list
	 */
	private void removeLocal(List<ApbMapKey> keyList){
		List<AntidoteMapEntry> entriesValid = new ArrayList<AntidoteMapEntry>();		
		for (AntidoteMapEntry e : getEntryList()){
			if (! keyList.contains(e.getPath().get(e.getPath().size()-1))){
				entriesValid.add(e);
			}
		}
		setEntryList(entriesValid);
	}

	/**
	 * Removes an entry locally.
	 *
	 * @param keyList the key list
	 */
	private void remove(List<String> keyList, CRDT_type type){
		List<ApbMapKey> apbKeyList = new ArrayList<ApbMapKey>();
		ApbMapKey.Builder apbKeyBuilder = ApbMapKey.newBuilder();
		apbKeyBuilder.setType(type);
		for (String key : keyList){
			apbKeyBuilder.setKey(ByteString.copyFromUtf8(key));
			ApbMapKey apbKey = apbKeyBuilder.build();
			apbKeyList.add(apbKey);
		}
		removeLocal(apbKeyList);
		addRemoveToList(apbKeyList);
	}
	
	/**
	 * Removes the counter.
	 *
	 * @param key the key
	 */
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
	
	/**
	 * Push locally executed updates to database.
	 */
	public void push(){
		for(Entry<List<ApbMapKey>, Entry<ApbMapKey, List<ApbUpdateOperation>>> update : getUpdateList()){
			if(update.getKey() != null){
				getClient().removeAWMapEntry(getName(), getBucket(), update.getKey());
			}
			else if(update.getValue().getKey() != null && update.getValue().getValue() != null){
				getClient().updateAWMap(getName(), getBucket(), update.getValue().getKey(), update.getValue().getValue());
			}
		}
		clearUpdateList();
	}
}