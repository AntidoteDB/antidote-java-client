package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.ApbUpdateOperation;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

/**
 * The Class AntidoteGMap.
 */
public class AntidoteGMap extends AntidoteMap implements GMapInterface{
	
	/**
	 * Instantiates a new antidote G map.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param entryList the map's entries
	 * @param antidoteClient the antidote client
	 */
	public AntidoteGMap(String name, String bucket, List<AntidoteMapEntry> entryList, AntidoteClient antidoteClient) {
		super(name, bucket, entryList, antidoteClient);
	}

	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){
		if (getUpdateList().size() > 0){
			throw new AntidoteException("You can't read the database without pushing your changes first or rolling back");
		}
		setEntryList(getClient().readGMap(getName(), getBucket()).getEntryList());
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
	 * Execute updates locally.
	 *
	 * @param key the key of the entry which is updated
	 * @param updateList updates which are executed on that entry
	 */
	public void updateLocal(String key, List<AntidoteMapUpdate> updateList){
		super.updateLocal(key, updateList, CRDT_type.GMAP);	
	}
	
	/**
	 * Update.
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
	 * Update.
	 *
	 * @param key the key of the entry which is updated
	 * @param updateList updates which are executed on that entry
	 */
	public void update(String key, List<AntidoteMapUpdate> updateList){
		ApbMapKey.Builder apbKeyBuilder = ApbMapKey.newBuilder();
		CRDT_type type = updateList.get(0).getType();
		for (AntidoteMapUpdate u : updateList){
			if (!(type.equals(u.getType()))){
				throw new  IllegalArgumentException("Different types detected, only one type allowed");
			}
		}
		apbKeyBuilder.setType(type);
		apbKeyBuilder.setKey(ByteString.copyFromUtf8(key));
		ApbMapKey apbKey = apbKeyBuilder.build();
		List<ApbUpdateOperation> apbUpdateList = new ArrayList<ApbUpdateOperation>();
		for (AntidoteMapUpdate u : updateList){
			apbUpdateList.add(u.getOperation());
		}	
		addUpdateToList(apbKey, apbUpdateList);
		updateLocal(key, updateList);		
	}
	
	/**
	 * Push locally executed updates to database. Uses a transaction.
	 */
	public void push(){
		AntidoteTransaction antidoteTransaction = new AntidoteTransaction(getClient());  
		ByteString descriptor = antidoteTransaction.startTransaction();		
		for(Entry<List<ApbMapKey>, Entry<ApbMapKey, List<ApbUpdateOperation>>> update : getUpdateList()){
			if(update.getValue().getKey() != null && update.getValue().getValue() != null){
				antidoteTransaction.updateGMapTransaction(
						getName(), getBucket(), update.getValue().getKey(), update.getValue().getValue(), descriptor);
			}		
		}
		antidoteTransaction.commitTransaction(descriptor);
		clearUpdateList();
	}
}
