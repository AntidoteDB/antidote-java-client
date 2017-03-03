package main.java.AntidoteClient;

import java.util.List;
import com.google.protobuf.ByteString;
import interfaces.GMapCRDT;

/**
 * The Class AntidoteOuterGMap.
 */
public final class AntidoteOuterGMap extends AntidoteOuterMap implements GMapCRDT{
	
	/** The low level map. */
	private final GMapRef lowLevelMap;
	
	/**
	 * Instantiates a new antidote G map.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param entryList the map's entries
	 * @param antidoteClient the antidote client
	 */
	public AntidoteOuterGMap(String name, String bucket, List<AntidoteInnerCRDT> entryList, AntidoteClient antidoteClient) {
		super(name, bucket, entryList, antidoteClient, AntidoteType.GMapType);
		lowLevelMap = new GMapRef(name, bucket, antidoteClient);
	}

	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(AntidoteTransaction antidoteTransaction){
		setEntryList(lowLevelMap.readEntryList(antidoteTransaction));
	}
	
	/**
	 * Update the entry with given key. Type information is contained in the AntidoteMapUpdate.
	 *
	 * @param key the key of the entry which is updated
	 * @param update the update which is executed on that entry
	 * @param antidoteTransaction the antidote transaction
	 */
	public void update(String key, AntidoteMapUpdate update, AntidoteTransaction antidoteTransaction){
		super.updateBS(ByteString.copyFromUtf8(key), update, AntidoteType.GMapType, antidoteTransaction);
	}

	/**
	 * Update the entry with given key. Type information is contained in the AntidoteMapUpdate.
	 *
	 * @param key the key of the entry which is updated
	 * @param updateList updates which are executed on that entry, type must be the same for all of them
	 * @param antidoteTransaction the antidote transaction
	 */
	public void update(String key, List<AntidoteMapUpdate> updateList, AntidoteTransaction antidoteTransaction){
		super.updateBS(ByteString.copyFromUtf8(key), updateList, AntidoteType.GMapType, antidoteTransaction);
	}
	
	/**
	 * Update the entry with given key. Type information is contained in the AntidoteMapUpdate.
	 *
	 * @param key the key of the entry which is updated
	 * @param update the update which is executed on that entry
	 * @param antidoteTransaction the antidote transaction
	 */
	public void updateBS(ByteString key, AntidoteMapUpdate update, AntidoteTransaction antidoteTransaction){
		super.updateBS(key, update, AntidoteType.GMapType, antidoteTransaction);
	}

	/**
	 * Update the entry with given key. Type information is contained in the AntidoteMapUpdate.
	 *
	 * @param key the key of the entry which is updated
	 * @param updateList updates which are executed on that entry, type must be the same for all of them
	 * @param antidoteTransaction the antidote transaction
	 */
	public void updateBS(ByteString key, List<AntidoteMapUpdate> updateList, AntidoteTransaction antidoteTransaction){
		super.updateBS(key, updateList, AntidoteType.GMapType, antidoteTransaction);
	}
}
