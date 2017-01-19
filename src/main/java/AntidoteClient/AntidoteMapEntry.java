package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.AbstractMap.SimpleEntry;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.ApbUpdateOperation;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;

// TODO: Auto-generated Javadoc
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
	
	/** The list of locally but not yet pushed operations. */
	private List<Map.Entry<CRDT_type, List<ApbUpdateOperation>>> updateList; 
	
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
	
	protected void clearUpdateList(){
		updateList.clear();
	}
	
	/**
	 * Update helper.
	 *
	 * @param innerUpdate the inner update
	 */
	public void updateHelper(List<AntidoteMapUpdate> innerUpdate){
		AntidoteMapUpdate mapUpdate;
		List<AntidoteMapUpdate> mapUpdateList = new ArrayList<AntidoteMapUpdate>();
		ApbMapKey apbKey;
		for (int i = getPath().size()-1; i>0; i--){
			apbKey = getPath().get(i);
			if (i == getPath().size()-1){
				mapUpdate = getClient().createAWMapUpdate(apbKey.getKey().toStringUtf8(), innerUpdate);
				mapUpdateList.add(mapUpdate);
			}
			else{
				if (getPath().get(i).getType() == CRDT_type.GMAP){
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
			List<ApbUpdateOperation> apbMapUpdateList = new ArrayList<ApbUpdateOperation>();
			for (AntidoteMapUpdate u : mapUpdateList){
				apbMapUpdateList.add(u.getOperation());
			}
			if (getOuterMapType() == CRDT_type.GMAP){
				updateList.add(new SimpleEntry<>(CRDT_type.GMAP, apbMapUpdateList));
			}
			else if (getOuterMapType() == CRDT_type.AWMAP){
				updateList.add(new SimpleEntry<>(CRDT_type.AWMAP, apbMapUpdateList));
			}
		}
		else if (getPath().size()==1){
			List<ApbUpdateOperation> apbInnerUpdate = new ArrayList<ApbUpdateOperation>();
			for (AntidoteMapUpdate u : innerUpdate){
				apbInnerUpdate.add(u.getOperation());
			}
			if (getOuterMapType() == CRDT_type.GMAP){
				updateList.add(new SimpleEntry<>(CRDT_type.GMAP, apbInnerUpdate));
			}
			else if (getOuterMapType() == CRDT_type.AWMAP){
				updateList.add(new SimpleEntry<>(CRDT_type.AWMAP, apbInnerUpdate));
			}
		}
	}
	
	
	/**
	 * Push locally executed updates to database.
	 */
	public void push(){
		for(Map.Entry<CRDT_type, List<ApbUpdateOperation>> update : updateList){
			if(update.getKey() == CRDT_type.GMAP){
				getClient().updateGMap(getName(), getBucket(), getPath().get(0), update.getValue());
			}
			else if(update.getKey() == CRDT_type.AWMAP){
				getClient().updateAWMap(getName(), getBucket(), getPath().get(0), update.getValue());
			}
		}
		updateList.clear();
	}
	
	public List<Map.Entry<CRDT_type, List<ApbUpdateOperation>>> getUpdateList(){
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
