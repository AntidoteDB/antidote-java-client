package eu.antidotedb.client;

import java.util.ArrayList;
import java.util.List;

import com.basho.riak.protobuf.AntidotePB.ApbGetMapResp;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.basho.riak.protobuf.AntidotePB.ApbMapEntry;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;

/**
 * The Class LowLevelGMap.
 */
public final class GMapRef extends MapRef{

	/**
	 * Instantiates a new low level G map.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param antidoteClient the antidote client
	 */
	public GMapRef(String name, String bucket, AntidoteClient antidoteClient){
		super(name, bucket, antidoteClient, AntidoteType.GMapType);
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
     * Read G map from database.
     *
     * @param antidoteTransaction the transaction
     * @return the antidote G map
     */
    public AntidoteOuterGMap createAntidoteGMap(AntidoteTransaction antidoteTransaction){
    	ApbGetMapResp map = antidoteTransaction.readHelper(getName(), getBucket(), getType()).getObjects(0).getMap();
        List<ApbMapEntry> apbEntryList = new ArrayList<ApbMapEntry>();
        apbEntryList = map.getEntriesList();
        List<AntidoteInnerCRDT> antidoteEntryList = new ArrayList<AntidoteInnerCRDT>();
    	List<ApbMapKey> path = new ArrayList<ApbMapKey>();
        antidoteEntryList = readMapHelper(path, apbEntryList, getType());
        return new AntidoteOuterGMap(getName(), getBucket(), antidoteEntryList, getClient());   
    }

	/**
	 * Read G map from database.
	 *
	 * @return the antidote G map
	 */
	public AntidoteOuterGMap createAntidoteGMap(){
		List<ApbMapEntry> apbEntryList = new ArrayList<ApbMapEntry>();
		apbEntryList = (List<ApbMapEntry>)getObjectRefValue(this);
		List<AntidoteInnerCRDT> antidoteEntryList = new ArrayList<AntidoteInnerCRDT>();
		List<ApbMapKey> path = new ArrayList<ApbMapKey>();
		antidoteEntryList = readMapHelper(path, apbEntryList, getType());
		return new AntidoteOuterGMap(getName(), getBucket(), antidoteEntryList, getClient());
	}
	
	/**
     * Read G map from database.
     *
     * @param antidoteTransaction the transaction
     * @return the antidote G map entry list
     */
    public List<AntidoteInnerCRDT> readEntryList(AntidoteTransaction antidoteTransaction){
    	ApbGetMapResp map = antidoteTransaction.readHelper(getName(), getBucket(), getType()).getObjects(0).getMap();
    	List<ApbMapEntry> apbEntryList = new ArrayList<ApbMapEntry>();
        apbEntryList = map.getEntriesList();
    	List<ApbMapKey> path = new ArrayList<ApbMapKey>();
        return readMapHelper(path, apbEntryList, getType());
    }

	/**
	 * Read G map from database.
	 *
	 * @return the antidote G map entry list
	 */
	public List<AntidoteInnerCRDT> readEntryList(){
		List<ApbMapEntry> apbEntryList = new ArrayList<ApbMapEntry>();
		apbEntryList = (List<ApbMapEntry>)getObjectRefValue(this);
		List<ApbMapKey> path = new ArrayList<ApbMapKey>();
		return readMapHelper(path, apbEntryList, getType());
	}

	public List<AntidoteInnerCRDT> readSetMapHelper(List<ApbMapEntry> apbEntryList){
		List<ApbMapKey> path = new ArrayList<ApbMapKey>();
		return readMapHelper(path, apbEntryList, getType());
	}
}
