package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.AbstractMap.SimpleEntry;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

/**
 * The Class AntidoteMapEntry.
 */
public class AntidoteMapEntry {
	
	/** The antidote client. */
	private AntidoteClient antidoteClient;
	
	/** The path storing the key and type of all inner maps leading to the entry. This is needed when storing Map entries in variables 
	* that are a subclass of AntidoteMapEntry if we want to give them an update method.*/
	private List<ApbMapKey> path;
	
	/** The name. */
	private String name;
	
	/** The bucket. */
	private String bucket;
	
	/** The type of the outer Map. */
	private CRDT_type outerMapType;
	
	/** The list of locally executed but not yet pushed operations. */
	private List<Map.Entry<CRDT_type, List<AntidoteMapUpdate>>> updateList; 
	
	/**
	 * Instantiates a new antidote map entry.
	 *
	 * @param antidoteClient the antidote client
	 * @param name the name
	 * @param bucket the bucket
	 * @param path the path
	 * @param outerMapType the outer map type
	 */
	public AntidoteMapEntry(AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path, CRDT_type outerMapType){
		this.name = name;
		this.antidoteClient = antidoteClient;
		this.bucket = bucket;
		this.path = path;
		this.outerMapType = outerMapType;
		updateList = new ArrayList<>();
	}
	
	/**
	 * Clear update list.
	 */
	protected void clearUpdateList(){
		updateList.clear();
	}
	
	/**
	 * Update helper.
	 *
	 * @param innerUpdate the inner update
	 */
	public void updateHelper(List<AntidoteMapUpdate> innerUpdate){
		AntidoteMapUpdate mapUpdate = null;
		List<AntidoteMapUpdate> mapUpdateList = new ArrayList<AntidoteMapUpdate>();
		ApbMapKey apbKey;
		for (int i = getPath().size()-1; i>0; i--){
			apbKey = getPath().get(i);
			if (i == getPath().size()-1){
				if (getPath().get(i-1).getType() == CRDT_type.GMAP){
					mapUpdate = getClient().createGMapUpdate(apbKey.getKey().toStringUtf8(), innerUpdate);
				}
				else{
					mapUpdate = getClient().createAWMapUpdate(apbKey.getKey().toStringUtf8(), innerUpdate);
				}
				mapUpdateList.add(mapUpdate);
			}
			else{
				if (getPath().get(i-1).getType() == CRDT_type.GMAP){
					mapUpdate = getClient().createGMapUpdate(apbKey.getKey().toStringUtf8(), mapUpdateList);
				}
				else{
					mapUpdate = getClient().createAWMapUpdate(apbKey.getKey().toStringUtf8(), mapUpdateList);
				}
				mapUpdateList = new ArrayList<AntidoteMapUpdate>();
				mapUpdateList.add(mapUpdate);
			}
		}
		if (getPath().size()>1){
			if (getOuterMapType() == CRDT_type.GMAP){
				updateList.add(new SimpleEntry<>(CRDT_type.GMAP, mapUpdateList));
			}
			else if (getOuterMapType() == CRDT_type.AWMAP){
				updateList.add(new SimpleEntry<>(CRDT_type.AWMAP, mapUpdateList));
			}
		}
		else if (getPath().size()==1){
			if (getOuterMapType() == CRDT_type.GMAP){
				updateList.add(new SimpleEntry<>(CRDT_type.GMAP, innerUpdate));
			}
			else if (getOuterMapType() == CRDT_type.AWMAP){
				updateList.add(new SimpleEntry<>(CRDT_type.AWMAP, innerUpdate));
			}
		}
	}
	
	
	/**
	 * Push locally executed updates to database. Uses a transaction.
	 */
	public void push(){
		AntidoteTransaction antidoteTransaction = new AntidoteTransaction(getClient());  
		ByteString descriptor = antidoteTransaction.startTransaction();		
		for(Map.Entry<CRDT_type, List<AntidoteMapUpdate>> update : updateList){
			if(update.getKey() == CRDT_type.GMAP){
				antidoteTransaction.updateGMapTransaction(getName(), getBucket(), new AntidoteMapKey(getPath().get(0)), update.getValue(), descriptor);
			}
			else if(update.getKey() == CRDT_type.AWMAP){
				antidoteTransaction.updateAWMapTransaction(getName(), getBucket(), new AntidoteMapKey(getPath().get(0)), update.getValue(), descriptor);
			}
		}
		antidoteTransaction.commitTransaction(descriptor);
		updateList.clear();
	}
	
	/**
	 * Gets the update list.
	 *
	 * @return the update list
	 */
	public List<Map.Entry<CRDT_type, List<AntidoteMapUpdate>>> getUpdateList(){
		return updateList;
	}
	
	/**
	 * Gets the name.
	 *
	 * @return the name
	 */
	public String getName(){
		return name;
	}
	
	/**
	 * Gets the bucket.
	 *
	 * @return the bucket
	 */
	public String getBucket(){
		return bucket;
	}
	
	/**
	 * Gets the path.
	 *
	 * @return the path
	 */
	public List<ApbMapKey> getPath(){
		return path;
	}
	
	/**
	 * Gets the client.
	 *
	 * @return the client
	 */
	public AntidoteClient getClient(){
		return antidoteClient;
	}
	
	/**
	 * Gets the outer map type.
	 *
	 * @return the outer map type
	 */
	public CRDT_type getOuterMapType(){
		return outerMapType;
	}
}
