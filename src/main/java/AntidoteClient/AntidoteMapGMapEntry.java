package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;

/**
 * The Class AntidoteMapGMapEntry.
 */
public class AntidoteMapGMapEntry extends AntidoteMapMapEntry {
	
	/**
	 * Instantiates a new antidote map G map entry.
	 *
	 * @param entryList the entry list
	 * @param antidoteClient the antidote client
	 * @param name the name
	 * @param bucket the bucket
	 * @param path the path
	 * @param outerMapType the outer map type
	 */
	public AntidoteMapGMapEntry(List<AntidoteMapEntry> entryList, AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path, CRDT_type outerMapType){
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
	 * @param update the update
	 */
	public void update(String mapKey, List<AntidoteMapUpdate> updateList){
		updateLocal(mapKey, updateList);
		List<AntidoteMapUpdate> innerMapUpdate = new ArrayList<AntidoteMapUpdate>(); 
		innerMapUpdate.add(getClient().createGMapUpdate(mapKey, updateList));
		updateHelper(innerMapUpdate);
	}
}
