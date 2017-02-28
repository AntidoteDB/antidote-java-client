package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;

import com.basho.riak.protobuf.AntidotePB.ApbGetMapResp;
import com.basho.riak.protobuf.AntidotePB.ApbMapEntry;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.ApbMapUpdate;
import com.basho.riak.protobuf.AntidotePB.ApbUpdateOperation;

/**
 * The Class LowLevelAWMap.
 */
public final class AWMapRef extends MapRef{
	
	/**
	 * Instantiates a new low level AW map.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param antidoteClient the antidote client
	 */
	public AWMapRef(String name, String bucket, AntidoteClient antidoteClient){
		super(name, bucket, antidoteClient);
	}
	
	/**
	 * Update.
	 *
	 * @param mapKey the map key
	 * @param update the update
	 */
	public void update(AntidoteMapKey mapKey, AntidoteMapUpdate update) {
	    super.update(mapKey, update, AntidoteType.AWMapType);
	}
		
	/**
	 * Update.
	 *
	 * @param mapKey the map key
	 * @param updates the updates
	 */
	public void update(AntidoteMapKey mapKey, List<AntidoteMapUpdate> updates) { 
	    super.update(mapKey, updates, AntidoteType.AWMapType);
	}
	
	/**
	 * Update.
	 *
	 * @param mapKey the map key
	 * @param update the update
	 * @param antidoteTransaction the antidote transaction
	 */
	public void update(AntidoteMapKey mapKey, AntidoteMapUpdate update, AntidoteTransaction antidoteTransaction) {
	    super.update(mapKey, update, AntidoteType.AWMapType, antidoteTransaction);
	}
		
	/**
	 * Update.
	 *
	 * @param mapKey the map key
	 * @param updates the updates
	 * @param antidoteTransaction the antidote transaction
	 */
	public void update(AntidoteMapKey mapKey, List<AntidoteMapUpdate> updates, AntidoteTransaction antidoteTransaction) { 
	    super.update(mapKey, updates, AntidoteType.AWMapType, antidoteTransaction);
	}
	
	/**
	 * Prepares a remove operation builder.
	 *
	 * @param keys the keys
	 * @return the apb update operation. builder
	 */
	protected ApbUpdateOperation.Builder removeOpBuilder(List<AntidoteMapKey> keys){
		ApbMapUpdate.Builder mapUpdateInstruction = ApbMapUpdate.newBuilder(); // The specific instruction in update instruction
        List<ApbMapKey> apbKeys = new ArrayList<>();
        ApbMapKey.Builder apbKeyBuilder = ApbMapKey.newBuilder();
        apbKeyBuilder.setType(keys.get(0).getType());
        for (AntidoteMapKey key : keys){
        	apbKeyBuilder.setKey(key.getKeyBS());
        	apbKeys.add(apbKeyBuilder.build());
        }
        mapUpdateInstruction.addAllRemovedKeys(apbKeys);      
        
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setMapop(mapUpdateInstruction);
        return updateOperation;
	}
	
    /**
     * Removes the element with given key.
     *
     * @param key the key
     */
    public void remove(AntidoteMapKey key) {
        List<AntidoteMapKey> keys = new ArrayList<>();
        keys.add(key);
        remove(keys);
    }
    
    /**
     * Removes the elements with given key.
     *
     * @param keys the keys
     */
    public void remove(List<AntidoteMapKey> keys) {
        updateHelper(removeOpBuilder(keys), getName(), getBucket(), AntidoteType.AWMapType);
    }
    
    /**
     * Removes the element with given key.
     *
     * @param key the key
     * @param antidoteTransaction the antidote transaction
     */
    public void remove(AntidoteMapKey key, AntidoteTransaction antidoteTransaction) {
        List<AntidoteMapKey> keys = new ArrayList<>();
        keys.add(key);
        remove(keys, antidoteTransaction);
    }
    
    /**
     * Removes the elements with given key.
     *
     * @param keys the keys
     * @param antidoteTransaction the antidote transaction
     */
    public void remove(List<AntidoteMapKey> keys, AntidoteTransaction antidoteTransaction) {
        ApbMapUpdate.Builder mapUpdateInstruction = ApbMapUpdate.newBuilder(); // The specific instruction in update instruction
        List<ApbMapKey> apbKeys = new ArrayList<>();
        ApbMapKey.Builder apbKeyBuilder = ApbMapKey.newBuilder();
        apbKeyBuilder.setType(keys.get(0).getType());
        for (AntidoteMapKey key : keys){
        	apbKeyBuilder.setKey(key.getKeyBS());
        	apbKeys.add(apbKeyBuilder.build());
        }
        mapUpdateInstruction.addAllRemovedKeys(apbKeys);      
        
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setMapop(mapUpdateInstruction);
        antidoteTransaction.updateHelper(updateOperation, getName(), getBucket(), AntidoteType.AWMapType);
    }
    
    /**
	 * Read AW map from database.
	 * 
	 * @return the antidote AW-Map
	 */
	public AntidoteOuterAWMap createAntidoteAWMap() {
        ApbGetMapResp map = readHelper(getName(), getBucket(), AntidoteType.AWMapType).getObjects().getObjects(0).getMap();
        List<ApbMapEntry> apbEntryList = new ArrayList<ApbMapEntry>();
        apbEntryList = map.getEntriesList();
        List<AntidoteInnerCRDT> antidoteEntryList = new ArrayList<AntidoteInnerCRDT>();
    	List<ApbMapKey> path = new ArrayList<ApbMapKey>();
        antidoteEntryList = readMapHelper(path, apbEntryList, AntidoteType.AWMapType);     
        return new AntidoteOuterAWMap(getName(), getBucket(), antidoteEntryList, getClient());   
    }
	
	/**
     * Read AW map from database.
     *
     * @param antidoteTransaction the transaction
     * @return the antidote AW map
     */
    public AntidoteOuterAWMap createAntidoteAWMap(AntidoteTransaction antidoteTransaction){
    	ApbGetMapResp map = readHelper(getName(), getBucket(), AntidoteType.AWMapType, antidoteTransaction).getObjects(0).getMap();
        List<ApbMapEntry> apbEntryList = new ArrayList<ApbMapEntry>();
        apbEntryList = map.getEntriesList();
        List<AntidoteInnerCRDT> antidoteEntryList = new ArrayList<AntidoteInnerCRDT>();
    	List<ApbMapKey> path = new ArrayList<ApbMapKey>();
        antidoteEntryList = readMapHelper(path, apbEntryList, AntidoteType.AWMapType);     
        return new AntidoteOuterAWMap(getName(), getBucket(), antidoteEntryList, getClient());   
    }
    
    /**
	 * Read AW map from database.
	 *
	 * @return the antidote AW-Map entry list
	 */
	public List<AntidoteInnerCRDT> readEntryList() {
        ApbGetMapResp map = readHelper(getName(), getBucket(), AntidoteType.AWMapType).getObjects().getObjects(0).getMap();
        List<ApbMapEntry> apbEntryList = new ArrayList<ApbMapEntry>();
        apbEntryList = map.getEntriesList();
    	List<ApbMapKey> path = new ArrayList<ApbMapKey>();
        return readMapHelper(path, apbEntryList, AntidoteType.AWMapType);  
    }
	
	/**
     * Read AW map from database.
     *
     * @param antidoteTransaction the transaction
     * @return the antidote AW map entry list
     */
    public List<AntidoteInnerCRDT> readEntryList(AntidoteTransaction antidoteTransaction){
    	ApbGetMapResp map = readHelper(getName(), getBucket(), AntidoteType.AWMapType, antidoteTransaction).getObjects(0).getMap();
    	List<ApbMapEntry> apbEntryList = new ArrayList<ApbMapEntry>();
        apbEntryList = map.getEntriesList();
    	List<ApbMapKey> path = new ArrayList<ApbMapKey>();
        return readMapHelper(path, apbEntryList, AntidoteType.AWMapType);    
    }
}
