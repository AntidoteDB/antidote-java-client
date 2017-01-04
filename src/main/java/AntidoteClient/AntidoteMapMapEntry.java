package main.java.AntidoteClient;

import static java.lang.Math.toIntExact;

import java.util.ArrayList;
import java.util.List;

import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.ApbMapNestedUpdate;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

/**
 * The Class AntidoteMapMapEntry.
 */
public class AntidoteMapMapEntry extends AntidoteMapEntry {
	
	/** The entry list. */
	private List<AntidoteMapEntry> entryList;
	
	/**
	 * Instantiates a new antidote map map entry.
	 *
	 * @param entryList the entry list
	 * @param antidoteClient the antidote client
	 * @param name the name
	 * @param bucket the bucket
	 * @param path the path
	 * @param outerMapType the outer map type
	 */
	public AntidoteMapMapEntry(List<AntidoteMapEntry> entryList, AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path, CRDT_type outerMapType){
		super(antidoteClient, name, bucket, path, outerMapType);
		this.entryList = entryList;
	}
	
	/**
	 * Gets the entry list.
	 *
	 * @return the entry list
	 */
	public List<AntidoteMapEntry> getEntryList(){
		return entryList;
	}
	
	/**
	 * Sets the entry list.
	 *
	 * @param entryList the new entry list
	 */
	public void setEntryList(List<AntidoteMapEntry> entryList){
		this.entryList = entryList;
	}
	
	/**
	 * Helper for readDatabase.
	 *
	 * @param outerMap the outer map
	 * @return the update helper
	 */
	public AntidoteMapMapEntry getUpdateHelper(AntidoteGMap outerMap){
		if (getPath().get(0).getType() == CRDT_type.GMAP){
			return outerMap.getGMapEntry(getPath().get(0).getKey().toStringUtf8());
		}
		else if(getPath().get(0).getType() == CRDT_type.AWMAP){
			return outerMap.getAWMapEntry(getPath().get(0).getKey().toStringUtf8());
		}
		return null;
	}
	
	/**
	 * Helper for readDatabase.
	 *
	 * @param outerMap the outer map
	 * @return the update helper
	 */
	public AntidoteMapMapEntry getUpdateHelper(AntidoteAWMap outerMap){
		if (getPath().get(0).getType() == CRDT_type.GMAP){
			return outerMap.getGMapEntry(getPath().get(0).getKey().toStringUtf8());
		}
		else if(getPath().get(0).getType() == CRDT_type.AWMAP){
			return outerMap.getAWMapEntry(getPath().get(0).getKey().toStringUtf8());
		}
		return null;
	}
	
	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){	
		AntidoteMapMapEntry innerMap = null;
		if (getOuterMapType() == CRDT_type.GMAP){
			AntidoteGMap outerMap = getClient().readGMap(getName(), getBucket());
			innerMap = getUpdateHelper(outerMap);
		}
		else if (getOuterMapType() == CRDT_type.AWMAP){
			AntidoteAWMap outerMap = getClient().readAWMap(getName(), getBucket());
			innerMap = getUpdateHelper(outerMap);
		}
		for (int i = 1; i<getPath().size()-1; i++){
			if (getPath().get(i).getType() == CRDT_type.GMAP){
				innerMap = innerMap.getGMapEntry(getPath().get(i).getKey().toStringUtf8());
			}
			else if(getPath().get(i).getType() == CRDT_type.AWMAP){
				innerMap = innerMap.getAWMapEntry(getPath().get(i).getKey().toStringUtf8());
			}
		}
		entryList = innerMap.getEntryList();
	}
	
	/**
	 * Execute updat locally.
	 *
	 * @param key the key
	 * @param updateList the update list
	 */
	public void updateLocal(String key, List<AntidoteMapUpdate> updateList){
		int i = 0;
		int index = -1;
		List<ApbMapKey> newPath = new ArrayList<ApbMapKey>();
		CRDT_type type = updateList.get(0).getType();
		ApbMapKey.Builder apbKeyBuilder = ApbMapKey.newBuilder();
		apbKeyBuilder.setType(type);
		apbKeyBuilder.setKey(ByteString.copyFromUtf8(key));
		ApbMapKey apbKey = apbKeyBuilder.build();
		newPath.addAll(getPath());
		newPath.add(apbKey);
		switch (type){
		case ORSET:
			AntidoteMapORSetEntry updatedORSetEntry = new AntidoteMapORSetEntry(new ArrayList<String>(), getClient(), getName(), getBucket(), newPath, getOuterMapType());
			for(AntidoteMapEntry e : entryList){
				//check if there already is an entry for given key
				//if so, overwrite the newly created one
				if (e.getPath().get(e.getPath().size()-1).getKey().toStringUtf8().equals(key) && e.getPath().get(e.getPath().size()-1).getType().equals(type)){
					updatedORSetEntry = (AntidoteMapORSetEntry) e;
					index = i;
					break;
				}
				i++;
			}
			for (AntidoteMapUpdate u : updateList){
				if (u.getOperation().getSetop().getAddsCount() > 0){
					for (ByteString add : u.getOperation().getSetop().getAddsList()){
						updatedORSetEntry.addElementLocal(add.toStringUtf8());
					}
				}
				else if (u.getOperation().getSetop().getRemsCount() > 0){
					for (ByteString add : u.getOperation().getSetop().getRemsList()){
						updatedORSetEntry.removeElementLocal(add.toStringUtf8());
					}	
				}
			}			
			if (index > -1){
				entryList.set(index, updatedORSetEntry);
			}
			else{
				entryList.add(updatedORSetEntry);
			}
			break;
		case RWSET:
			AntidoteMapRWSetEntry updatedRWSetEntry = new AntidoteMapRWSetEntry(new ArrayList<String>(), getClient(), getName(), getBucket(), newPath, getOuterMapType());
			for(AntidoteMapEntry e : entryList){
				//check if there already is an entry for given key
				//if so, overwrite the newly created one
				if (e.getPath().get(e.getPath().size()-1).getKey().toStringUtf8().equals(key) && e.getPath().get(e.getPath().size()-1).getType().equals(type)){
					updatedRWSetEntry = (AntidoteMapRWSetEntry) e;
					index = i;
					break;
				}
				i++;
			}
			for (AntidoteMapUpdate u : updateList){
				if (u.getOperation().getSetop().getAddsCount() > 0){
					for (ByteString add : u.getOperation().getSetop().getAddsList()){
						updatedRWSetEntry.addElementLocal(add.toStringUtf8());
					}
				}
				else if (u.getOperation().getSetop().getRemsCount() > 0){
					for (ByteString add : u.getOperation().getSetop().getRemsList()){
						updatedRWSetEntry.removeElementLocal(add.toStringUtf8());
					}	
				}
			}			
			if (index > -1){
				entryList.set(index, updatedRWSetEntry);
			}
			else{
				entryList.add(updatedRWSetEntry);
			}
			break;
		case COUNTER:
			AntidoteMapCounterEntry updatedCounterEntry = new AntidoteMapCounterEntry(0, getClient(), getName(), getBucket(), newPath, getOuterMapType());
			for(AntidoteMapEntry e : entryList){
				//check if there already is an entry for given key
				//if so, overwrite the newly created one
				if (e.getPath().get(e.getPath().size()-1).getKey().toStringUtf8().equals(key) && e.getPath().get(e.getPath().size()-1).getType().equals(type)){
					updatedCounterEntry = (AntidoteMapCounterEntry) e;
					index = i;
					break;
				}
				i++;
			}
			for (AntidoteMapUpdate u : updateList){
				updatedCounterEntry.incrementLocal(toIntExact(u.getOperation().getCounterop().getInc()));
			}			
			if (index > -1){
				entryList.set(index, updatedCounterEntry);
			}
			else{
				entryList.add(updatedCounterEntry);
			}
			break;
		case INTEGER:
			AntidoteMapIntegerEntry updatedIntegerEntry = new AntidoteMapIntegerEntry(0, getClient(), getName(), getBucket(), newPath, getOuterMapType());
			for(AntidoteMapEntry e : entryList){
				//check if there already is an entry for given key
				//if so, overwrite the newly created one
				if (e.getPath().get(e.getPath().size()-1).getKey().toStringUtf8().equals(key) && e.getPath().get(e.getPath().size()-1).getType().equals(type)){
					updatedIntegerEntry = (AntidoteMapIntegerEntry) e;
					index = i;
					break;
				}
				i++;
			}
			for (AntidoteMapUpdate u : updateList){
				if (u.getOperation().getIntegerop().getInc() != 0){
					updatedIntegerEntry.incrementLocal(toIntExact(u.getOperation().getIntegerop().getInc()));
				}
				else{
					System.out.println(u.getOperation().getIntegerop().getSet());
					updatedIntegerEntry.setLocal(toIntExact(u.getOperation().getIntegerop().getSet()));
				}
			}			
			if (index > -1){
				entryList.set(index, updatedIntegerEntry);
			}
			else{
				entryList.add(updatedIntegerEntry);
			}
			break;	
		case LWWREG:
			AntidoteMapRegisterEntry updatedRegisterEntry = new AntidoteMapRegisterEntry("", getClient(), getName(), getBucket(), newPath, getOuterMapType());
			for(AntidoteMapEntry e : entryList){
				//check if there already is an entry for given key
				//if so, overwrite the newly created one
				if (e.getPath().get(e.getPath().size()-1).getKey().toStringUtf8().equals(key) && e.getPath().get(e.getPath().size()-1).getType().equals(type)){
					updatedRegisterEntry = (AntidoteMapRegisterEntry) e;
					index = i;
					break;
				}
				i++;
			}
			for (AntidoteMapUpdate u : updateList){
				updatedRegisterEntry.setLocal(u.getOperation().getRegop().getValue().toStringUtf8());
			}			
			if (index > -1){
				entryList.set(index, updatedRegisterEntry);
			}
			else{
				entryList.add(updatedRegisterEntry);
			}
			break;
		case MVREG:
			AntidoteMapMVRegisterEntry updatedMVRegisterEntry = new AntidoteMapMVRegisterEntry(
					new ArrayList<String>(), getClient(), getName(), getBucket(), newPath, getOuterMapType());
			for(AntidoteMapEntry e : entryList){
				//check if there already is an entry for given key
				//if so, overwrite the newly created one
				if (e.getPath().get(e.getPath().size()-1).getKey().toStringUtf8().equals(key) && e.getPath().get(e.getPath().size()-1).getType().equals(type)){
					updatedMVRegisterEntry = (AntidoteMapMVRegisterEntry) e;
					index = i;
					break;
				}
				i++;
			}
			for (AntidoteMapUpdate u : updateList){
				updatedMVRegisterEntry.setLocal(u.getOperation().getRegop().getValue().toStringUtf8());
			}			
			if (index > -1){
				entryList.set(index, updatedMVRegisterEntry);
			}
			else{
				entryList.add(updatedMVRegisterEntry);
			}
			break;
		case AWMAP:
			AntidoteMapAWMapEntry updatedAWMapEntry = new AntidoteMapAWMapEntry(new ArrayList<AntidoteMapEntry>(), getClient(), getName(), getBucket(), newPath, getOuterMapType());
			for(AntidoteMapEntry e : entryList){
				//check if there already is an entry for given key
				//if so, overwrite the newly created one
				if (e.getPath().get(e.getPath().size()-1).getKey().toStringUtf8().equals(key) && e.getPath().get(e.getPath().size()-1).getType().equals(type)){
					updatedAWMapEntry = (AntidoteMapAWMapEntry) e;
					index = i;
					break;
				}
				i++;
			}
			for (AntidoteMapUpdate u : updateList){
				if(u.getOperation().getMapop().getRemovedKeysList().size() > 0){
					updatedAWMapEntry.removeLocal(u.getOperation().getMapop().getRemovedKeysList());

				}
				else{
					List<AntidoteMapUpdate> tempList = new ArrayList<AntidoteMapUpdate>();
			    	for (ApbMapNestedUpdate update : u.getOperation().getMapop().getUpdatesList()){			
				    	tempList.add(new AntidoteMapUpdate(update.getKey().getType(), update.getUpdate()));
			    	}
			    	updatedAWMapEntry.updateLocal(u.getOperation().getMapop().getUpdates(0).getKey().getKey().toStringUtf8(), tempList);
				}
			}			
			if (index > -1){
				entryList.set(index, updatedAWMapEntry);
			}
			else{
				entryList.add(updatedAWMapEntry);
			}
			break;
		case GMAP:
			AntidoteMapGMapEntry updatedGMapEntry = new AntidoteMapGMapEntry(new ArrayList<AntidoteMapEntry>(), getClient(), getName(), getBucket(), newPath, getOuterMapType());
			for(AntidoteMapEntry e : entryList){
				//check if there already is an entry for given key
				//if so, overwrite the newly created one
				if (e.getPath().get(e.getPath().size()-1).getKey().toStringUtf8().equals(key) && e.getPath().get(e.getPath().size()-1).getType().equals(type)){
					updatedGMapEntry = (AntidoteMapGMapEntry) e;
					index = i;
					break;
				}
				i++;
			}
			for (AntidoteMapUpdate u : updateList){
				List<AntidoteMapUpdate> tempList = new ArrayList<AntidoteMapUpdate>();
			    for (ApbMapNestedUpdate update : u.getOperation().getMapop().getUpdatesList()){			
				   	tempList.add(new AntidoteMapUpdate(update.getKey().getType(), update.getUpdate()));
			    }
			    updatedGMapEntry.updateLocal(u.getOperation().getMapop().getUpdates(0).getKey().getKey().toStringUtf8(), tempList);
			}			
			if (index > -1){
				entryList.set(index, updatedGMapEntry);
			}
			else{
				entryList.add(updatedGMapEntry);
			}
			break;
		}	
	}
	
	/**
	 * Gets the counter entry.
	 *
	 * @param key the key
	 * @return the counter entry
	 */
	public AntidoteMapCounterEntry getCounterEntry(String key){
		for (AntidoteMapEntry e : entryList){
			if (e.getPath().get(e.getPath().size()-1).getType() == CRDT_type.COUNTER && e.getPath().get(e.getPath().size()-1).getKey().toStringUtf8().equals(key)){
				return (AntidoteMapCounterEntry)e;		
			}
		}
		return null;
	}
	
	/**
	 * Gets the OR set entry.
	 *
	 * @param key the key
	 * @return the OR set entry
	 */
	public AntidoteMapORSetEntry getORSetEntry(String key){
		for (AntidoteMapEntry e : entryList){
			if (e.getPath().get(e.getPath().size()-1).getType() == CRDT_type.ORSET && e.getPath().get(e.getPath().size()-1).getKey().toStringUtf8().equals(key)){
				return (AntidoteMapORSetEntry)e;		
			}
		}
		return null;
	}
	
	/**
	 * Gets the RW set entry.
	 *
	 * @param key the key
	 * @return the RW set entry
	 */
	public AntidoteMapRWSetEntry getRWSetEntry(String key){
		for (AntidoteMapEntry e : entryList){
			if (e.getPath().get(e.getPath().size()-1).getType() == CRDT_type.RWSET && e.getPath().get(e.getPath().size()-1).getKey().toStringUtf8().equals(key)){
				return (AntidoteMapRWSetEntry)e;		
			}
		}
		return null;
	}
	
	/**
	 * Gets the register entry.
	 *
	 * @param key the key
	 * @return the register entry
	 */
	public AntidoteMapRegisterEntry getRegisterEntry(String key){
		for (AntidoteMapEntry e : entryList){
			if (e.getPath().get(e.getPath().size()-1).getType() == CRDT_type.LWWREG && e.getPath().get(e.getPath().size()-1).getKey().toStringUtf8().equals(key)){
				return (AntidoteMapRegisterEntry)e;		
			}
		}
		return null;
	}
	
	/**
	 * Gets the MV register entry.
	 *
	 * @param key the key
	 * @return the MV register entry
	 */
	public AntidoteMapMVRegisterEntry getMVRegisterEntry(String key){
		for (AntidoteMapEntry e : entryList){
			if (e.getPath().get(e.getPath().size()-1).getType() == CRDT_type.MVREG && e.getPath().get(e.getPath().size()-1).getKey().toStringUtf8().equals(key)){
				return (AntidoteMapMVRegisterEntry)e;		
			}
		}
		return null;
	}
	
	/**
	 * Gets the integer entry.
	 *
	 * @param key the key
	 * @return the integer entry
	 */
	public AntidoteMapIntegerEntry getIntegerEntry(String key){
		for (AntidoteMapEntry e : entryList){
			if (e.getPath().get(e.getPath().size()-1).getType() == CRDT_type.INTEGER && e.getPath().get(e.getPath().size()-1).getKey().toStringUtf8().equals(key)){
				return (AntidoteMapIntegerEntry)e;		
			}
		}
		return null;
	}
	
	/**
	 * Gets the AW map entry.
	 *
	 * @param key the key
	 * @return the AW map entry
	 */
	public AntidoteMapAWMapEntry getAWMapEntry(String key){
		for (AntidoteMapEntry e : entryList){
			if (e.getPath().get(e.getPath().size()-1).getType() == CRDT_type.AWMAP && e.getPath().get(e.getPath().size()-1).getKey().toStringUtf8().equals(key)){
				return (AntidoteMapAWMapEntry)e;		
			}
		}
		return null;
	}
	
	/**
	 * Gets the g map entry.
	 *
	 * @param key the key
	 * @return the g map entry
	 */
	public AntidoteMapGMapEntry getGMapEntry(String key){
		for (AntidoteMapEntry e : entryList){
			if (e.getPath().get(e.getPath().size()-1).getType() == CRDT_type.GMAP && e.getPath().get(e.getPath().size()-1).getKey().toStringUtf8().equals(key)){
				return (AntidoteMapGMapEntry)e;		
			}
		}
		return null;
	}
}
