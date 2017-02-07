package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;

import com.basho.riak.protobuf.AntidotePB.ApbGetMapResp;
import com.basho.riak.protobuf.AntidotePB.ApbMapEntry;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;

/**
 * The Class LowLevelGMap.
 */
public class LowLevelGMap extends LowLevelMap{
	
	/**
	 * Instantiates a new low level G map.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param antidoteClient the antidote client
	 */
	public LowLevelGMap(String name, String bucket, AntidoteClient antidoteClient){
		super(name, bucket, antidoteClient);
	}
	
	/**
	 * Update.
	 *
	 * @param mapKey the map key
	 * @param update the update
	 */
	public void update(AntidoteMapKey mapKey, AntidoteMapUpdate update) {
	    super.update(mapKey, update, AntidoteType.GMapType);
	}
		
	/**
	 * Update.
	 *
	 * @param mapKey the map key
	 * @param updates the updates
	 */
	public void update(AntidoteMapKey mapKey, List<AntidoteMapUpdate> updates) { 
	    super.update(mapKey, updates, AntidoteType.GMapType);
	}
	
	/**
	 * Update.
	 *
	 * @param mapKey the map key
	 * @param update the update
	 * @param antidoteTransaction the antidote transaction
	 */
	public void update(AntidoteMapKey mapKey, AntidoteMapUpdate update, AntidoteTransaction antidoteTransaction) {
	    super.update(mapKey, update, AntidoteType.GMapType, antidoteTransaction);
	}
		
	/**
	 * Update.
	 *
	 * @param mapKey the map key
	 * @param updates the updates
	 * @param antidoteTransaction the antidote transaction
	 */
	public void update(AntidoteMapKey mapKey, List<AntidoteMapUpdate> updates, AntidoteTransaction antidoteTransaction) { 
	    super.update(mapKey, updates, AntidoteType.GMapType, antidoteTransaction);
	}
	
	/**
	 * Read G map from database.
	 * 
	 * @return the antidote G-Map
	 */
	public AntidoteOuterGMap createAntidoteGMap() {
        ApbGetMapResp map = getClient().readHelper(getName(), getBucket(), AntidoteType.GMapType).getObjects().getObjects(0).getMap();
        List<ApbMapEntry> apbEntryList = new ArrayList<ApbMapEntry>();
        apbEntryList = map.getEntriesList();
        List<AntidoteInnerObject> antidoteEntryList = new ArrayList<AntidoteInnerObject>();
    	List<ApbMapKey> path = new ArrayList<ApbMapKey>();
        antidoteEntryList = readMapHelper(path, apbEntryList, AntidoteType.GMapType);     
        return new AntidoteOuterGMap(getName(), getBucket(), antidoteEntryList, getClient());   
    }
	
	/**
     * Read G map from database.
     *
     * @param antidoteTransaction the transaction
     * @return the antidote G map
     */
    public AntidoteOuterGMap createAntidoteGMap(AntidoteTransaction antidoteTransaction){
    	ApbGetMapResp map = antidoteTransaction.readHelper(getName(), getBucket(), AntidoteType.GMapType).getObjects(0).getMap();
        List<ApbMapEntry> apbEntryList = new ArrayList<ApbMapEntry>();
        apbEntryList = map.getEntriesList();
        List<AntidoteInnerObject> antidoteEntryList = new ArrayList<AntidoteInnerObject>();
    	List<ApbMapKey> path = new ArrayList<ApbMapKey>();
        antidoteEntryList = readMapHelper(path, apbEntryList, AntidoteType.GMapType);     
        return new AntidoteOuterGMap(getName(), getBucket(), antidoteEntryList, getClient());   
    }
    
    /**
	 * Read G map from database.
	 *
	 * @return the antidote G-Map entry list
	 */
	public List<AntidoteInnerObject> readEntryList() {
        ApbGetMapResp map = getClient().readHelper(getName(), getBucket(), AntidoteType.GMapType).getObjects().getObjects(0).getMap();
        List<ApbMapEntry> apbEntryList = new ArrayList<ApbMapEntry>();
        apbEntryList = map.getEntriesList();
    	List<ApbMapKey> path = new ArrayList<ApbMapKey>();
        return readMapHelper(path, apbEntryList, AntidoteType.GMapType);  
    }
	
	/**
     * Read G map from database.
     *
     * @param antidoteTransaction the transaction
     * @return the antidote G map entry list
     */
    public List<AntidoteInnerObject> readEntryList(AntidoteTransaction antidoteTransaction){
    	ApbGetMapResp map = antidoteTransaction.readHelper(getName(), getBucket(), AntidoteType.GMapType).getObjects(0).getMap();
    	List<ApbMapEntry> apbEntryList = new ArrayList<ApbMapEntry>();
        apbEntryList = map.getEntriesList();
    	List<ApbMapKey> path = new ArrayList<ApbMapKey>();
        return readMapHelper(path, apbEntryList, AntidoteType.GMapType);    
    }
}
