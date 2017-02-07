package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;

/**
 * The Class AntidoteOuterAWMap.
 */
public class AntidoteOuterAWMap extends AntidoteOuterMap implements InterfaceAWMap{
	
	/** The low level AW-map. */
	private LowLevelAWMap lowLevelMap;
	/**
	 * Instantiates a new antidote AW map.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param entryList the map's entries
	 * @param antidoteClient the antidote client
	 */
	public AntidoteOuterAWMap(String name, String bucket, List<AntidoteInnerObject> entryList, AntidoteClient antidoteClient) {
		super(name, bucket, entryList, antidoteClient, AntidoteType.AWMapType);
		lowLevelMap = new LowLevelAWMap(name, bucket, antidoteClient);
	}
	
	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){
		if (getUpdateList().size() > 0){
			throw new AntidoteException("You can't read the database without pushing your changes first or rolling back");
		}
		setEntryList(lowLevelMap.readEntryList());
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.AWMapInterface#rollBack()
	 */
	public void rollBack(){
		clearUpdateList();
		readDatabase();
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.AWMapInterface#synchronize()
	 */
	public void synchronize(){
		push();
		readDatabase();
	}
	
	/**
	 * Update the entry with given key. Type information is contained in the AntidoteMapUpdate.
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
	 * Update the entry with given key. Type information is contained in the AntidoteMapUpdate.
	 *
	 * @param key the key of the entry which is updated
	 * @param updateList updates which are executed on that entry, type must be the same for all of them
	 */
	public void update(String key, List<AntidoteMapUpdate> updateList){
		super.update(key, updateList, AntidoteType.AWMapType);
	}
	
	/**
	 * Removes entries locally.
	 *
	 * @param keyList the key list
	 */
	private void removeLocal(List<AntidoteMapKey> keyList){
		List<AntidoteInnerObject> entriesValid = new ArrayList<AntidoteInnerObject>();		
		for (AntidoteInnerObject e : getEntryList()){
			if (! keyList.contains(new AntidoteMapKey(e.getPath().get(e.getPath().size()-1)))){
				entriesValid.add(e);
			}
		}
		setEntryList(entriesValid);
	}
	
	/**
	 * Removes an entry of given type.
	 *
	 * @param key the key
	 * @param type the type, use AntidoteType._Type in the method call (AntidoteType.CounterType for example)
	 */
	public void remove(String key, CRDT_type type){
		List<String> keyList = new ArrayList<String>();
		keyList.add(key);
		remove(keyList, type);
	}

	/**
	 * Removes a list of entries of the same type.
	 *
	 * @param keyList the key list
     * @param type the type, use AntidoteType._Type in the method call (AntidoteType.CounterType for example)
	 */
	public void remove(List<String> keyList, CRDT_type type){
		List<AntidoteMapKey> mapKeyList = new ArrayList<AntidoteMapKey>();
		for (String key : keyList){
			mapKeyList.add(new AntidoteMapKey(type, key));
		}
		removeLocal(mapKeyList);
		addRemoveToList(mapKeyList);
	}
}