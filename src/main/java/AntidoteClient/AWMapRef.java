package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;

import com.basho.riak.protobuf.AntidotePB.CRDT_type;
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
		super(name, bucket, antidoteClient, AntidoteType.AWMapType);
	}
	
	/**
	 * Update.
	 *
	 * @param mapKey the map key
	 * @param update the update
	 * @param antidoteTransaction the antidote transaction
	 */
	public void update(AntidoteMapKey mapKey, AntidoteMapUpdate update, AntidoteTransaction antidoteTransaction) {
	    super.update(mapKey, update, getType(), antidoteTransaction);
	}
		
	/**
	 * Update.
	 *
	 * @param mapKey the map key
	 * @param updates the updates
	 * @param antidoteTransaction the antidote transaction
	 */
	public void update(AntidoteMapKey mapKey, List<AntidoteMapUpdate> updates, AntidoteTransaction antidoteTransaction) { 
	    super.update(mapKey, updates, getType(), antidoteTransaction);
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
        antidoteTransaction.updateHelper(removeOpBuilder(keys), getName(), getBucket(), getType());
    }
	
	/**
     * Read AW map from database.
     *
     * @param antidoteTransaction the transaction
     * @return the antidote AW map
     */
    public AntidoteOuterAWMap createAntidoteAWMap(AntidoteTransaction antidoteTransaction){
    	ApbGetMapResp map = antidoteTransaction.readHelper(getName(), getBucket(), getType()).getObjects(0).getMap();
        List<ApbMapEntry> apbEntryList = new ArrayList<ApbMapEntry>();
        apbEntryList = map.getEntriesList();
        List<AntidoteInnerCRDT> antidoteEntryList = new ArrayList<AntidoteInnerCRDT>();
    	List<ApbMapKey> path = new ArrayList<ApbMapKey>();
        antidoteEntryList = readMapHelper(path, apbEntryList, getType());
        return new AntidoteOuterAWMap(getName(), getBucket(), antidoteEntryList, getClient());   
    }

	/**
	 * Read AW map from database.
	 *
	 * @return the antidote AW map
	 */
	public AntidoteOuterAWMap createAntidoteAWMap() {
		List<ApbMapEntry> apbEntryList = new ArrayList<ApbMapEntry>();
		apbEntryList = (List<ApbMapEntry>) getObjectRefValue(this);
		List<AntidoteInnerCRDT> antidoteEntryList = new ArrayList<AntidoteInnerCRDT>();
		List<ApbMapKey> path = new ArrayList<ApbMapKey>();
		antidoteEntryList = readMapHelper(path, apbEntryList, getType());
		return new AntidoteOuterAWMap(getName(), getBucket(), antidoteEntryList, getClient());
	}

	/**
     * Read AW map from database.
     *
     * @param antidoteTransaction the transaction
     * @return the antidote AW map entry list
     */
    public List<AntidoteInnerCRDT> readEntryList(AntidoteTransaction antidoteTransaction){
    	ApbGetMapResp map = antidoteTransaction.readHelper(getName(), getBucket(),getType()).getObjects(0).getMap();
    	List<ApbMapEntry> apbEntryList = new ArrayList<ApbMapEntry>();
        apbEntryList = map.getEntriesList();
    	List<ApbMapKey> path = new ArrayList<ApbMapKey>();
        return readMapHelper(path, apbEntryList, getType());
    }

	/**
	 * Read AW map from database.
	 *
	 * @return the antidote AW map entry list
	 */
	public List<AntidoteInnerCRDT> readEntryList(){
		List<ApbMapEntry> apbEntryList = new ArrayList<ApbMapEntry>();
		apbEntryList = (List<ApbMapEntry>) getObjectRefValue(this);
		List<ApbMapKey> path = new ArrayList<ApbMapKey>();
		return readMapHelper(path, apbEntryList, getType());
	}

	public List<AntidoteInnerCRDT> readSetMapHelper(List<ApbMapEntry> apbEntryList){
		List<ApbMapKey> path = new ArrayList<ApbMapKey>();
		return readMapHelper(path, apbEntryList, getType());
	}
}
