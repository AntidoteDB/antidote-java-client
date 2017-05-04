package eu.antidotedb.client;

import java.util.ArrayList;
import java.util.List;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;

/**
 * The Class AntidoteInnerObject.
 */
public class AntidoteInnerCRDT extends AntidoteCRDT{
	
	/** The path storing the key and type of all inner maps leading to the entry. This is needed when storing Map entries in variables 
	* that are a subclass of AntidoteMapEntry if we want to give them an update method.*/
	private final List<ApbMapKey> path;
	
	/**
	 * Instantiates a new antidote map entry.
	 *
	 * @param antidoteClient the antidote client
	 * @param name the name
	 * @param bucket the bucket
	 * @param path the path
	 * @param outerMapType the outer map type
	 */
	public AntidoteInnerCRDT(AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path, CRDT_type outerMapType){
		super(name, bucket, antidoteClient, outerMapType);
		this.path = path;
	}

	/**
	 * Update helper.
	 *
	 * @param innerUpdate the inner update
	 * @param antidoteTransaction the antidote transaction
	 */
	protected void updateHelper(List<AntidoteMapUpdate> innerUpdate, AntidoteTransaction antidoteTransaction){
		AntidoteMapUpdate mapUpdate = null;
		List<AntidoteMapUpdate> mapUpdateList = new ArrayList<AntidoteMapUpdate>();
		ApbMapKey apbKey;
		for (int i = getPath().size()-1; i>0; i--){
			apbKey = getPath().get(i);
			if (i == getPath().size()-1){
				if (getPath().get(i-1).getType() == AntidoteType.GMapType){
					mapUpdate = AntidoteMapUpdate.createGMapUpdate(apbKey.getKey().toStringUtf8(), innerUpdate);
				}
				else{
					mapUpdate = AntidoteMapUpdate.createAWMapUpdate(apbKey.getKey().toStringUtf8(), innerUpdate);
				}
				mapUpdateList.add(mapUpdate);
			}
			else{
				if (getPath().get(i-1).getType() == AntidoteType.GMapType){
					mapUpdate = AntidoteMapUpdate.createGMapUpdate(apbKey.getKey().toStringUtf8(), mapUpdateList);
				}
				else{
					mapUpdate = AntidoteMapUpdate.createAWMapUpdate(apbKey.getKey().toStringUtf8(), mapUpdateList);
				}
				mapUpdateList = new ArrayList<AntidoteMapUpdate>();
				mapUpdateList.add(mapUpdate);
			}
		}
		if (getPath().size()>1){
			if (getType() == AntidoteType.GMapType){
				antidoteTransaction.updateHelper(new MapRef(getName(), getBucket(), getClient(), getType()).updateOpBuilder(new AntidoteMapKey(getPath().get(0)), mapUpdateList),getName(),getBucket(),getType());
			}
			else if (getType() == AntidoteType.AWMapType){
				antidoteTransaction.updateHelper(new MapRef(getName(), getBucket(), getClient(), getType()).updateOpBuilder(new AntidoteMapKey(getPath().get(0)), mapUpdateList),getName(),getBucket(),getType());
			}
		}
		else if (getPath().size()==1){
			if (getType() == AntidoteType.GMapType){
				antidoteTransaction.updateHelper(new MapRef(getName(), getBucket(), getClient(), getType()).updateOpBuilder(new AntidoteMapKey(getPath().get(0)), innerUpdate),getName(),getBucket(),getType());
			}
			else if (getType() == AntidoteType.AWMapType){
				antidoteTransaction.updateHelper(new MapRef(getName(), getBucket(), getClient(), getType()).updateOpBuilder(new AntidoteMapKey(getPath().get(0)), innerUpdate),getName(),getBucket(),getType());
			}
		}
	}

	/**
	 * Read database helper.
	 *
	 * @param path the path
	 * @param outerMap the outer map
	 * @return the antidote inner map
	 */
	protected AntidoteInnerMap readDatabaseHelper(List<ApbMapKey> path, AntidoteOuterGMap outerMap){
		AntidoteInnerMap innerMap = null;
		if (getPath().get(0).getType()==AntidoteType.AWMapType){
			innerMap = outerMap.getAWMapEntry(getPath().get(0).getKey().toStringUtf8());
		}
		else if (getPath().get(0).getType()==AntidoteType.GMapType){
			innerMap = outerMap.getGMapEntry(getPath().get(0).getKey().toStringUtf8());
		}
		for (int i = 1; i<getPath().size()-1; i++){
			if (getPath().get(i).getType()==AntidoteType.AWMapType){
				innerMap = innerMap.getAWMapEntry(getPath().get(i).getKey().toStringUtf8());
			}
			else if (getPath().get(i).getType()==AntidoteType.GMapType){
				innerMap = innerMap.getGMapEntry(getPath().get(i).getKey().toStringUtf8());
			}
		}
		return innerMap;
	}
	
	/**
	 * Read database helper.
	 *
	 * @param path the path
	 * @param outerMap the outer map
	 * @return the antidote inner map
	 */
	protected AntidoteInnerMap readDatabaseHelper(List<ApbMapKey> path, AntidoteOuterAWMap outerMap){
		AntidoteInnerMap innerMap = null;
		if (getPath().get(0).getType()==AntidoteType.AWMapType){
			innerMap = outerMap.getAWMapEntry(getPath().get(0).getKey().toStringUtf8());
		}
		else if (getPath().get(0).getType()==AntidoteType.GMapType){
			innerMap = outerMap.getGMapEntry(getPath().get(0).getKey().toStringUtf8());
		}
		for (int i = 1; i<getPath().size()-1; i++){
			if (getPath().get(i).getType()==AntidoteType.AWMapType){
				innerMap = innerMap.getAWMapEntry(getPath().get(i).getKey().toStringUtf8());
			}
			else if (getPath().get(i).getType()==AntidoteType.GMapType){
				innerMap = innerMap.getGMapEntry(getPath().get(i).getKey().toStringUtf8());
			}
		}
		return innerMap;
	}
	
	/**
	 * Gets the path.
	 *
	 * @return the path
	 */
	public List<ApbMapKey> getPath(){
		return path;
	}
}
