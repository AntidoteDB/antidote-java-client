package eu.antidotedb.client;

import java.util.ArrayList;
import java.util.List;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

import eu.antidotedb.client.crdt.AWMapCRDT;

/**
 * The Class AntidoteInnerAWMap.
 */
public final class AntidoteInnerAWMap extends AntidoteInnerMap implements AWMapCRDT{
	
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
	public AntidoteInnerAWMap(List<AntidoteInnerCRDT> entryList, AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path, CRDT_type outerMapType){
		super(entryList, antidoteClient, name, bucket, path, outerMapType);
	}

	/**
	 * Update the entry with the given key.
	 *
	 * @param mapKey the map key
	 * @param update the update
	 * @param antidoteTransaction the antidote transaction
	 */
	public void update(String mapKey, AntidoteMapUpdate update, AntidoteTransaction antidoteTransaction){
		updateBS(ByteString.copyFromUtf8(mapKey), update, antidoteTransaction);
	}

	/**
	 * Update the entry with the given key with multiple updates. Type of AntidoteMapUpdate mustn't change
	 *
	 * @param mapKey the map key
	 * @param updateList the update list
	 * @param antidoteTransaction the antidote transaction
	 */
	public void update(String mapKey, List<AntidoteMapUpdate> updateList, AntidoteTransaction antidoteTransaction){
		updateBS(ByteString.copyFromUtf8(mapKey), updateList, antidoteTransaction);
	}
	
	/**
	 * Update the entry with the given key.
	 *
	 * @param mapKey the map key
	 * @param update the update
	 * @param antidoteTransaction the antidote transaction
	 */
	public void updateBS(ByteString mapKey, AntidoteMapUpdate update, AntidoteTransaction antidoteTransaction){
		List<AntidoteMapUpdate> updateList = new ArrayList<AntidoteMapUpdate>();
		updateList.add(update);
		updateBS(mapKey, updateList, antidoteTransaction);
	}

	/**
	 * Update the entry with the given key with multiple updates. Type of AntidoteMapUpdate mustn't change
	 *
	 * @param mapKey the map key
	 * @param updateList the update list
	 * @param antidoteTransaction the antidote transaction
	 */
	public void updateBS(ByteString mapKey, List<AntidoteMapUpdate> updateList, AntidoteTransaction antidoteTransaction){
		updateLocal(mapKey, updateList);
		List<AntidoteMapUpdate> innerMapUpdate = new ArrayList<AntidoteMapUpdate>();
		innerMapUpdate.add(AntidoteMapUpdate.createAWMapUpdateBS(mapKey, updateList));
		updateHelper(innerMapUpdate, antidoteTransaction);
	}
	
	/**
	 * Removes entries locally.
	 *
	 * @param keyList the key list
	 */
	protected void removeLocal(List<ApbMapKey> keyList) {
		List<AntidoteInnerCRDT> entriesValid = new ArrayList<AntidoteInnerCRDT>();		
		for (AntidoteInnerCRDT e : getEntryList()){
			if (! keyList.contains(e.getPath().get(e.getPath().size()-1))){
				entriesValid.add(e);
			}
		}
		setEntryList(entriesValid);
	}

	public void remove(String key, CRDT_type type, AntidoteTransaction antidoteTransaction){
		removeBS(ByteString.copyFromUtf8(key), type, antidoteTransaction);
	}

	/**
	 * Removes a list of entries with the same type.
	 *
	 * @param keyList the key list
	 * @param type the type
	 * @param antidoteTransaction the antidote transaction
	 */
	public void remove(List<String> keyList, CRDT_type type, AntidoteTransaction antidoteTransaction) {
		List<ByteString> keyListBS = new ArrayList<>();
		for(String s : keyList){
			keyListBS.add(ByteString.copyFromUtf8(s));
		}
		removeBS(keyListBS, type, antidoteTransaction);
	}
	
	public void removeBS(ByteString key, CRDT_type type, AntidoteTransaction antidoteTransaction){
		List<ByteString> keyList = new ArrayList<>();
		keyList.add(key);
		removeBS(keyList, type, antidoteTransaction);
	}

	/**
	 * Removes a list of entries with the same type.
	 *
	 * @param keyList the key list
	 * @param type the type
	 * @param antidoteTransaction the antidote transaction
	 */
	public void removeBS(List<ByteString> keyList, CRDT_type type, AntidoteTransaction antidoteTransaction) {
		List<ApbMapKey> apbKeyList = new ArrayList<ApbMapKey>();
		ApbMapKey.Builder apbKeyBuilder = ApbMapKey.newBuilder();
		apbKeyBuilder.setType(type);
		for (ByteString key : keyList){
			apbKeyBuilder.setKey(key);
			ApbMapKey apbKey = apbKeyBuilder.build();
			apbKeyList.add(apbKey);
		}
		removeLocal(apbKeyList);
		List<AntidoteMapUpdate> innerMapUpdate = new ArrayList<AntidoteMapUpdate>();
		innerMapUpdate.add(AntidoteMapUpdate.createMapRemoveBS(keyList, type));
		AntidoteMapUpdate mapUpdate;
		List<AntidoteMapUpdate> mapUpdateList = new ArrayList<AntidoteMapUpdate>();
		ApbMapKey key;
		for (int i = getPath().size()-1; i>0; i--){
			key = getPath().get(i);
			if (i == getPath().size()-1){
				mapUpdate = AntidoteMapUpdate.createAWMapUpdate(key.getKey().toStringUtf8(), innerMapUpdate);
				mapUpdateList.add(mapUpdate);
			}
			else{
				mapUpdate = null; //since we are not at the last position of path, it is one of both kinds of maps
				if(key.getType() == AntidoteType.AWMapType){
					mapUpdate = AntidoteMapUpdate.createAWMapUpdate(key.getKey().toStringUtf8(), mapUpdateList);
				}
				else if (key.getType() == AntidoteType.GMapType){
					mapUpdate = AntidoteMapUpdate.createGMapUpdate(key.getKey().toStringUtf8(), mapUpdateList);
				}
				mapUpdateList = new ArrayList<AntidoteMapUpdate>();
				mapUpdateList.add(mapUpdate);
			}
		}
		if (getPath().size()>1){
			if(getType() == AntidoteType.AWMapType){
				AWMapRef lowMap = new AWMapRef(getName(), getBucket(), getClient());
				lowMap.update(new AntidoteMapKey(getPath().get(0).getType(), getPath().get(0).getKey()), mapUpdateList, antidoteTransaction);
			}
			else{
				GMapRef lowMap = new GMapRef(getName(), getBucket(), getClient());
				lowMap.update(new AntidoteMapKey(getPath().get(0).getType(), getPath().get(0).getKey()), mapUpdateList, antidoteTransaction);
			}
		}
		else if (getPath().size()==1){
			if(getType() == AntidoteType.AWMapType){
				AWMapRef lowMap = new AWMapRef(getName(), getBucket(), getClient());
				lowMap.update(new AntidoteMapKey(getPath().get(0).getType(), getPath().get(0).getKey()), innerMapUpdate, antidoteTransaction);
			}
			else{
				GMapRef lowMap = new GMapRef(getName(), getBucket(), getClient());
				lowMap.update(new AntidoteMapKey(getPath().get(0).getType(), getPath().get(0).getKey()), innerMapUpdate, antidoteTransaction);
			}
		}
	}
}