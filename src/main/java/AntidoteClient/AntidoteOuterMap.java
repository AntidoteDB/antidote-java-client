package main.java.AntidoteClient;

import static java.lang.Math.toIntExact;

import java.util.ArrayList;
import java.util.List;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.ApbMapNestedUpdate;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

/**
 * The Class AntidoteOuterMap.
 */
public class AntidoteOuterMap extends AntidoteObject {
	
	/** The map's entries. */
	private List<AntidoteInnerObject> entryList;
	
	/**
	 * Instantiates a new antidote map.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param entryList the map's entries
	 * @param antidoteClient the antidote client
	 * @param type the type
	 */
	public AntidoteOuterMap(String name, String bucket, List<AntidoteInnerObject> entryList, AntidoteClient antidoteClient, CRDT_type type){
		super(name, bucket, antidoteClient, type);
		this.entryList = entryList;
	}
	
	/**
	 * Adds a remove operation to the updateList.
	 *
	 * @param keys the keys
	 */
	protected void addRemoveToList(List<AntidoteMapKey> keys){
		updateAdd(new LowLevelAWMap(getName(), getBucket(), getClient()).removeOpBuilder(keys));
	}
	
	/**
	 * Adds an update operation to the updateList.
	 *
	 * @param mapKey the map key
	 * @param update the update
	 */
	protected void addUpdateToList(AntidoteMapKey mapKey, List<AntidoteMapUpdate> update){
		updateAdd(new LowLevelMap(getName(), getBucket(), getClient()).updateOpBuilder(mapKey, update));	
	}
	
	/**
	 * Gets the map's entries.
	 *
	 * @return the entry list
	 */
	public List<AntidoteInnerObject> getEntryList(){
		return entryList;
	}
	
	/**
	 * Sets the map's entries.
	 *
	 * @param entryList the new entry list
	 */
	public void setEntryList(List<AntidoteInnerObject> entryList){
		this.entryList = entryList;
	}
	
	/**
	 * Update.
	 *
	 * @param key the key of the entry which is updated
	 * @param updateList the updates which are executed on that entry, must all be of the same type
	 * @param mapType the type of the map
	 */
	public void update(String key, List<AntidoteMapUpdate> updateList, CRDT_type mapType){
		CRDT_type type = updateList.get(0).getType();
		for (AntidoteMapUpdate u : updateList){
			if (!(type.equals(u.getType()))){
				throw new  IllegalArgumentException("Different types detected, only one type allowed");
			}
		}
		AntidoteMapKey mapKey = new AntidoteMapKey(type, key);
		addUpdateToList(mapKey, updateList);
		updateLocal(key, updateList, mapType);		
	}
	
	/**
	 * Execute an update locally.
	 *
	 * @param key the key of the entry to be updated
	 * @param updateList the list of updates executed on that entry
	 * @param mapType the map type
	 */
	protected void updateLocal(String key, List<AntidoteMapUpdate> updateList, CRDT_type mapType){
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
			AntidoteInnerORSet updatedORSetEntry = new AntidoteInnerORSet(new ArrayList<String>(), getClient(), getName(), getBucket(), newPath, mapType);
			for(AntidoteInnerObject e : entryList){
				//check if there already is an entry for given key
				//if so, overwrite the newly created one
				if (e.getPath().get(0).getKey().toStringUtf8().equals(key) && e.getPath().get(0).getType().equals(type)){
					updatedORSetEntry = (AntidoteInnerORSet) e;
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
			AntidoteInnerRWSet updatedRWSetEntry = new AntidoteInnerRWSet(new ArrayList<String>(), getClient(), getName(), getBucket(), newPath, mapType);
			for(AntidoteInnerObject e : entryList){
				//check if there already is an entry for given key
				//if so, overwrite the newly created one
				if (e.getPath().get(0).getKey().toStringUtf8().equals(key) && e.getPath().get(0).getType().equals(type)){
					updatedRWSetEntry = (AntidoteInnerRWSet) e;
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
			AntidoteInnerCounter updatedCounterEntry = new AntidoteInnerCounter(0, getClient(), getName(), getBucket(), newPath, mapType);
			for(AntidoteInnerObject e : entryList){
				//check if there already is an entry for given key
				//if so, overwrite the newly created one
				if (e.getPath().get(0).getKey().toStringUtf8().equals(key) && e.getPath().get(0).getType().equals(type)){
					updatedCounterEntry = (AntidoteInnerCounter) e;
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
			AntidoteInnerInteger updatedIntegerEntry = new AntidoteInnerInteger(0, getClient(), getName(), getBucket(), newPath, mapType);
			for(AntidoteInnerObject e : entryList){
				//check if there already is an entry for given key
				//if so, overwrite the newly created one
				if (e.getPath().get(0).getKey().toStringUtf8().equals(key) && e.getPath().get(0).getType().equals(type)){
					updatedIntegerEntry = (AntidoteInnerInteger) e;
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
			AntidoteInnerLWWRegister updatedRegisterEntry = new AntidoteInnerLWWRegister("", getClient(), getName(), getBucket(), newPath, mapType);
			for(AntidoteInnerObject e : entryList){
				//check if there already is an entry for given key
				//if so, overwrite the newly created one
				if (e.getPath().get(0).getKey().toStringUtf8().equals(key) && e.getPath().get(0).getType().equals(type)){
					updatedRegisterEntry = (AntidoteInnerLWWRegister) e;
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
			AntidoteInnerMVRegister updatedMVRegisterEntry = new AntidoteInnerMVRegister(
					new ArrayList<String>(), getClient(), getName(), getBucket(), newPath, mapType);
			for(AntidoteInnerObject e : entryList){
				//check if there already is an entry for given key
				//if so, overwrite the newly created one
				if (e.getPath().get(0).getKey().toStringUtf8().equals(key) && e.getPath().get(0).getType().equals(type)){
					updatedMVRegisterEntry = (AntidoteInnerMVRegister) e;
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
			AntidoteInnerAWMap updatedAWMapEntry = new AntidoteInnerAWMap(
					new ArrayList<AntidoteInnerObject>(), getClient(), getName(), getBucket(), newPath, mapType);
			for(AntidoteInnerObject e : entryList){
				//check if there already is an entry for given key
				//if so, overwrite the newly created one
				if (e.getPath().get(0).getKey().toStringUtf8().equals(key) && e.getPath().get(0).getType().equals(type)){
					updatedAWMapEntry = (AntidoteInnerAWMap) e;
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
			AntidoteInnerGMap updatedGMapEntry = new AntidoteInnerGMap(
					new ArrayList<AntidoteInnerObject>(), getClient(), getName(), getBucket(), newPath, mapType);
			for(AntidoteInnerObject e : entryList){
				//check if there already is an entry for given key
				//if so, overwrite the newly created one
				if (e.getPath().get(0).getKey().toStringUtf8().equals(key) && e.getPath().get(0).getType().equals(type)){
					updatedGMapEntry = (AntidoteInnerGMap) e;
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
	public AntidoteInnerCounter getCounterEntry(String key){
		for (AntidoteInnerObject e : entryList){
			if (e.getPath().get(0).getType() == AntidoteType.CounterType && e.getPath().get(0).getKey().toStringUtf8().equals(key)){
				return (AntidoteInnerCounter)e;		
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
	public AntidoteInnerORSet getORSetEntry(String key){
		for (AntidoteInnerObject e : entryList){
			if (e.getPath().get(0).getType() == AntidoteType.ORSetType && e.getPath().get(0).getKey().toStringUtf8().equals(key)){
				return (AntidoteInnerORSet)e;		
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
	public AntidoteInnerRWSet getRWSetEntry(String key){
		for (AntidoteInnerObject e : entryList){
			if (e.getPath().get(0).getType() == AntidoteType.RWSetType && e.getPath().get(0).getKey().toStringUtf8().equals(key)){
				return (AntidoteInnerRWSet)e;		
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
	public AntidoteInnerLWWRegister getLWWRegisterEntry(String key){
		for (AntidoteInnerObject e : entryList){
			if (e.getPath().get(0).getType() == AntidoteType.LWWRegisterType && e.getPath().get(0).getKey().toStringUtf8().equals(key)){
				return (AntidoteInnerLWWRegister)e;		
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
	public AntidoteInnerMVRegister getMVRegisterEntry(String key){
		for (AntidoteInnerObject e : entryList){
			if (e.getPath().get(0).getType() == AntidoteType.MVRegisterType && e.getPath().get(0).getKey().toStringUtf8().equals(key)){
				return (AntidoteInnerMVRegister)e;		
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
	public AntidoteInnerInteger getIntegerEntry(String key){
		for (AntidoteInnerObject e : entryList){
			if (e.getPath().get(0).getType() == AntidoteType.IntegerType && e.getPath().get(0).getKey().toStringUtf8().equals(key)){
				return (AntidoteInnerInteger)e;		
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
	public AntidoteInnerAWMap getAWMapEntry(String key){
		for (AntidoteInnerObject e : entryList){
			if (e.getPath().get(0).getType() == AntidoteType.AWMapType && e.getPath().get(0).getKey().toStringUtf8().equals(key)){
				return (AntidoteInnerAWMap)e;		
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
	public AntidoteInnerGMap getGMapEntry(String key){
		for (AntidoteInnerObject e : entryList){
			if (e.getPath().get(0).getType() == AntidoteType.GMapType && e.getPath().get(0).getKey().toStringUtf8().equals(key)){
				return (AntidoteInnerGMap)e;		
			}
		}
		return null;
	}
}
