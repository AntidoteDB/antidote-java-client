package main.java.AntidoteClient;

import static java.lang.Math.toIntExact;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.ApbMapNestedUpdate;
import com.basho.riak.protobuf.AntidotePB.ApbUpdateOperation;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

/**
 * The Class AntidoteMap.
 */
public class AntidoteMap extends AntidoteObject {
	
	/** The list of locally but not yet pushed operations. */
	// first Map.Entry for removes, second Map.Entry for updates. We need to use one list so the order of the operations is preserved when pushing to the database
	private List<Map.Entry<List<ApbMapKey>, Map.Entry<ApbMapKey, List<ApbUpdateOperation>>>> updateList;	
	
	/** The map's entries. */
	private List<AntidoteMapEntry> entryList;
	
	/**
	 * Instantiates a new antidote map.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param entryList the map's entries
	 * @param antidoteClient the antidote client
	 */
	public AntidoteMap(String name, String bucket, List<AntidoteMapEntry> entryList, AntidoteClient antidoteClient){
		super(name, bucket, antidoteClient);
		this.entryList = entryList;
		updateList = new ArrayList<>();
	}
	
	/**
	 * Gets the update list.
	 *
	 * @return the update list
	 */
	public List<Entry<List<ApbMapKey>, Entry<ApbMapKey, List<ApbUpdateOperation>>>> getUpdateList(){
		return updateList;
	}
	
	/**
	 * Adds a remove operation to the updateList.
	 *
	 * @param remove the remove
	 */
	public void addRemoveToList(List<ApbMapKey> remove){
		Map.Entry<ApbMapKey, List<ApbUpdateOperation>> notNeededPart = null;
		Map.Entry<List<ApbMapKey>, Map.Entry<ApbMapKey, List<ApbUpdateOperation>>> completeRemove = new SimpleEntry<>(remove, notNeededPart);
		updateList.add(completeRemove);
	}
	
	/**
	 * Adds an update operation to the updateList.
	 *
	 * @param mapKey the map key
	 * @param update the update
	 */
	public void addUpdateToList(ApbMapKey mapKey, List<ApbUpdateOperation> update){
		Map.Entry<ApbMapKey, List<ApbUpdateOperation>> updatePart = new SimpleEntry<>(mapKey, update);
		Map.Entry<List<ApbMapKey>, Map.Entry<ApbMapKey, List<ApbUpdateOperation>>> completeUpdate = new SimpleEntry<>(null, updatePart);
		updateList.add(completeUpdate);	}
	
	/**
	 * Clear update list.
	 */
	public void clearUpdateList(){
		updateList.clear();
	}
	
	/**
	 * Gets the map's entries.
	 *
	 * @return the entry list
	 */
	public List<AntidoteMapEntry> getEntryList(){
		return entryList;
	}
	
	/**
	 * Sets the map's entries.
	 *
	 * @param entryList the new entry list
	 */
	public void setEntryList(List<AntidoteMapEntry> entryList){
		this.entryList = entryList;
	}
	
	/**
	 * Execute an update locally.
	 *
	 * @param key the key of the entry to be updated
	 * @param updateList the list of updates executed on that entry
	 * @param outerMapType the outer map's type
	 */
	public void updateLocal(String key, List<AntidoteMapUpdate> updateList, CRDT_type outerMapType){
		int i = 0;
		int index = -1;
		List<ApbMapKey> newPath = new ArrayList<ApbMapKey>();
		CRDT_type type = updateList.get(0).getType();
		ApbMapKey.Builder apbKeyBuilder = ApbMapKey.newBuilder();
		apbKeyBuilder.setType(type);
		apbKeyBuilder.setKey(ByteString.copyFromUtf8(key));
		ApbMapKey apbKey = apbKeyBuilder.build();
		newPath.add(apbKey);
		switch (type){
		case ORSET:
			AntidoteMapORSetEntry updatedORSetEntry = new AntidoteMapORSetEntry(new ArrayList<String>(), getClient(), getName(), getBucket(), newPath, outerMapType);
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
				// there are two different possibilities for updating a set, so we need to find out, which one applies here
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
			AntidoteMapRWSetEntry updatedRWSetEntry = new AntidoteMapRWSetEntry(new ArrayList<String>(), getClient(), getName(), getBucket(), newPath, outerMapType);
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
				// there are two different possibilities for updating a set, so we need to find out, which one applies here
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
			AntidoteMapCounterEntry updatedCounterEntry = new AntidoteMapCounterEntry(0, getClient(), getName(), getBucket(), newPath, outerMapType);
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
			AntidoteMapIntegerEntry updatedIntegerEntry = new AntidoteMapIntegerEntry(0, getClient(), getName(), getBucket(), newPath, outerMapType);
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
				// there are two different possibilities for updating an integer, so we need to find out, which one applies here
				// we can assume that the integer is set if the increment is equal to zero because creating such an increment results in an exception
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
			AntidoteMapRegisterEntry updatedRegisterEntry = new AntidoteMapRegisterEntry("", getClient(), getName(), getBucket(), newPath, outerMapType);
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
					new ArrayList<String>(), getClient(), getName(), getBucket(), newPath, outerMapType);
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
			AntidoteMapAWMapEntry updatedAWMapEntry = new AntidoteMapAWMapEntry(
					new ArrayList<AntidoteMapEntry>(), getClient(), getName(), getBucket(), newPath, outerMapType);
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
				// there are two different possibilities for updating an AW-Map, so we need to find out, which one applies here
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
			AntidoteMapGMapEntry updatedGMapEntry = new AntidoteMapGMapEntry(
					new ArrayList<AntidoteMapEntry>(), getClient(), getName(), getBucket(), newPath, outerMapType);
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
		default:
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
