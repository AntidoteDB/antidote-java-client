package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;

/**
 * The Class AntidoteOuterGMap.
 */
public class AntidoteOuterGMap extends AntidoteOuterMap implements InterfaceGMap{
	
	/** The low level map. */
	private LowLevelGMap lowLevelMap;
	
	/**
	 * Instantiates a new antidote G map.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param entryList the map's entries
	 * @param antidoteClient the antidote client
	 */
	public AntidoteOuterGMap(String name, String bucket, List<AntidoteInnerObject> entryList, AntidoteClient antidoteClient) {
		super(name, bucket, entryList, antidoteClient, AntidoteType.GMapType);
		lowLevelMap = new LowLevelGMap(name, bucket, antidoteClient);
	}

	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){
		if (getUpdateList().size() > 0){
			throw new AntidoteException("You can't read the database without pushing your changes first or rolling back");
		}
		setEntryList(lowLevelMap.readEntryList());
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.GMapInterface#rollBack()
	 */
	public void rollBack(){
		clearUpdateList();
		readDatabase();
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.GMapInterface#synchronize()
	 */
	public void synchronize(){
		push();
		readDatabase();
	}
	
	/**
	 * Update the entry with given key. Type information is contained in the AntidoteMapUpdate.
	 *
	 * @param key the key of the entry which is updated
	 * @param update the update which is executed on that entry
	 */
	public void update(String key, AntidoteMapUpdate update){
		List<AntidoteMapUpdate> updateList = new ArrayList<AntidoteMapUpdate>();
		updateList.add(update);
		update(key, updateList);
	}
	
	/**
	 * Update the entry with given key. Type information is contained in the AntidoteMapUpdate.
	 *
	 * @param key the key of the entry which is updated
	 * @param updateList updates which are executed on that entry, type must be the same for all of them
	 */
	public void update(String key, List<AntidoteMapUpdate> updateList){
		super.update(key, updateList, AntidoteType.GMapType);	
	}
}
