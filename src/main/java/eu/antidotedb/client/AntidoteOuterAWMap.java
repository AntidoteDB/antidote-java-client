package eu.antidotedb.client;

import java.util.ArrayList;
import java.util.List;

import com.basho.riak.protobuf.AntidotePB.ApbMapEntry;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;


import eu.antidotedb.client.crdt.AWMapCRDT;

/**
 * The Class AntidoteOuterAWMap.
 */
public final class AntidoteOuterAWMap extends AntidoteOuterMap implements AWMapCRDT{
	
	/** The low level AW-map. */
	private final AWMapRef lowLevelMap;
	/**
	 * Instantiates a new antidote AW map.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param entryList the map's entries
	 * @param antidoteClient the antidote client
	 */
	public AntidoteOuterAWMap(String name, String bucket, List<AntidoteInnerCRDT> entryList, AntidoteClient antidoteClient) {
		super(name, bucket, entryList, antidoteClient, AntidoteType.AWMapType);
		lowLevelMap = new AWMapRef(name, bucket, antidoteClient);
	}
	
	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(AntidoteTransaction antidoteTransaction){
		setEntryList(lowLevelMap.readEntryList(antidoteTransaction));
	}

	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){
		setEntryList(lowLevelMap.readEntryList());
	}

	protected void readSetValue(List<ApbMapEntry> apbMapEntries){
		setEntryList(lowLevelMap.readSetMapHelper(new ArrayList<>(apbMapEntries)));
	}

	/**
	 * Update the entry with given key. Type information is contained in the AntidoteMapUpdate.
	 *
	 * @param key the key of the entry which is updated
	 * @param update the update which is executed on that entry
	 * @param antidoteTransaction the antidote transaction
	 */
	public void update(String key, AntidoteMapUpdate update, AntidoteTransaction antidoteTransaction){
		super.updateBS(ByteString.copyFromUtf8(key), update, AntidoteType.AWMapType, antidoteTransaction);
	}

	/**
	 * Update the entry with given key. Type information is contained in the AntidoteMapUpdate.
	 *
	 * @param key the key of the entry which is updated
	 * @param updateList updates which are executed on that entry, type must be the same for all of them
	 * @param antidoteTransaction the antidote transaction
	 */
	public void update(String key, List<AntidoteMapUpdate> updateList, AntidoteTransaction antidoteTransaction){
		super.updateBS(ByteString.copyFromUtf8(key), updateList, AntidoteType.AWMapType, antidoteTransaction);
	}
	
	/**
	 * Update the entry with given key. Type information is contained in the AntidoteMapUpdate.
	 *
	 * @param key the key of the entry which is updated
	 * @param update the update which is executed on that entry
	 * @param antidoteTransaction the antidote transaction
	 */
	public void updateBS(ByteString key, AntidoteMapUpdate update, AntidoteTransaction antidoteTransaction){
		super.updateBS(key, update, AntidoteType.AWMapType, antidoteTransaction);
	}

	/**
	 * Update the entry with given key. Type information is contained in the AntidoteMapUpdate.
	 *
	 * @param key the key of the entry which is updated
	 * @param updateList updates which are executed on that entry, type must be the same for all of them
	 * @param antidoteTransaction the antidote transaction
	 */
	public void updateBS(ByteString key, List<AntidoteMapUpdate> updateList, AntidoteTransaction antidoteTransaction){
		super.updateBS(key, updateList, AntidoteType.AWMapType, antidoteTransaction);
	}

	/**
	 * Removes entries locally.
	 *
	 * @param keyList the key list
	 */
	private void removeLocal(List<AntidoteMapKey> keyList){
		List<AntidoteInnerCRDT> entriesValid = new ArrayList<AntidoteInnerCRDT>();		
		for (AntidoteInnerCRDT e : getEntryList()){
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
	 * @param antidoteTransaction the antidote transaction
	 */
	public void remove(String key, CRDT_type type,AntidoteTransaction antidoteTransaction){
		removeBS(ByteString.copyFromUtf8(key), type, antidoteTransaction);
	}

	/**
	 * Removes a list of entries of the same type.
	 *
	 * @param keyList the key list
	 * @param type the type, use AntidoteType._Type in the method call (AntidoteType.CounterType for example)
	 * @param antidoteTransaction the antidote transaction
	 */
	public void remove(List<String> keyList, CRDT_type type, AntidoteTransaction antidoteTransaction){
		List<AntidoteMapKey> mapKeyList = new ArrayList<AntidoteMapKey>();
		for (String key : keyList){
			mapKeyList.add(new AntidoteMapKey(type, key));
		}
		removeLocal(mapKeyList);
		addRemoveToList(mapKeyList, antidoteTransaction);
	}
	
	/**
	 * Removes an entry of given type.
	 *
	 * @param key the key
	 * @param type the type, use AntidoteType._Type in the method call (AntidoteType.CounterType for example)
	 * @param antidoteTransaction the antidote transaction
	 */
	public void removeBS(ByteString key, CRDT_type type,AntidoteTransaction antidoteTransaction){
		List<ByteString> keyList = new ArrayList<>();
		keyList.add(key);
		removeBS(keyList, type, antidoteTransaction);
	}

	/**
	 * Removes a list of entries of the same type.
	 *
	 * @param keyList the key list
	 * @param type the type, use AntidoteType._Type in the method call (AntidoteType.CounterType for example)
	 * @param antidoteTransaction the antidote transaction
	 */
	public void removeBS(List<ByteString> keyList, CRDT_type type, AntidoteTransaction antidoteTransaction){
		List<AntidoteMapKey> mapKeyList = new ArrayList<AntidoteMapKey>();
		for (ByteString key : keyList){
			mapKeyList.add(new AntidoteMapKey(type, key));
		}
		removeLocal(mapKeyList);
		addRemoveToList(mapKeyList, antidoteTransaction);
	}
}