package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

import interfaces.AWMapCRDT;

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
	 */
	public void update(String mapKey, AntidoteMapUpdate update){
		List<AntidoteMapUpdate> updateList = new ArrayList<AntidoteMapUpdate>();
		updateList.add(update);
		update(mapKey, updateList);
	}
	
	/**
	 * Update the entry with the given key with multiple updates. Type of AntidoteMapUpdate mustn't change
	 *
	 * @param mapKey the map key
	 * @param updateList the update list
	 */
	public void update(String mapKey, List<AntidoteMapUpdate> updateList){
		updateLocal(mapKey, updateList);
		List<AntidoteMapUpdate> innerMapUpdate = new ArrayList<AntidoteMapUpdate>(); 
		innerMapUpdate.add(AntidoteMapUpdate.createAWMapUpdate(mapKey, updateList));
		updateHelper(innerMapUpdate);
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

	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.AWMapInterface#remove(java.lang.String, com.basho.riak.protobuf.AntidotePB.CRDT_type)
	 */
	public void remove(String key, CRDT_type type){
		List<String> keyList = new ArrayList<>();
		keyList.add(key);
		remove(keyList, type);
	}
	
	/**
	 * Removes a list of entries with the same type.
	 *
	 * @param keyList the key list
	 * @param type the type
	 */
	public void remove(List<String> keyList, CRDT_type type) {
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
		innerMapUpdate.add(AntidoteMapUpdate.createMapRemove(keyList, type));
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
				lowMap.update(new AntidoteMapKey(getPath().get(0).getType(), getPath().get(0).getKey()), mapUpdateList);
			}
			else{
				GMapRef lowMap = new GMapRef(getName(), getBucket(), getClient());
				lowMap.update(new AntidoteMapKey(getPath().get(0).getType(), getPath().get(0).getKey()), mapUpdateList);
			}
		}
		else if (getPath().size()==1){
			if(getType() == AntidoteType.AWMapType){
				AWMapRef lowMap = new AWMapRef(getName(), getBucket(), getClient());
				lowMap.update(new AntidoteMapKey(getPath().get(0).getType(), getPath().get(0).getKey()), innerMapUpdate);
			}
			else{
				GMapRef lowMap = new GMapRef(getName(), getBucket(), getClient());
				lowMap.update(new AntidoteMapKey(getPath().get(0).getType(), getPath().get(0).getKey()), innerMapUpdate);
			}
		}		
	}
}