package main.java.AntidoteClient;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;

import java.util.ArrayList;
import java.util.List;

/**
 * The Class AntidoteInnerCounter.
 */
public class AntidoteInnerCounter extends AntidoteInnerObject implements InterfaceCounter{
	
	/** The value. */
	private int value;
	
	/**
	 * Instantiates a new antidote map counter entry.
	 *
	 * @param value the value
	 * @param antidoteClient the antidote client
	 * @param name the name
	 * @param bucket the bucket
	 * @param path the path
	 * @param outerMapType the outer map type
	 */
	public AntidoteInnerCounter(int value, AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path, CRDT_type outerMapType){
		super(antidoteClient, name, bucket, path, outerMapType);
		this.value = value;
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.CounterInterface#rollBack()
	 */
	public void rollBack(){
		clearUpdateList();
		readDatabase();
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.CounterInterface#synchronize()
	 */
	public void synchronize(){
		push();
		readDatabase();
	}	
	
	
	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){
		if (getUpdateList().size() > 0){
			throw new AntidoteException("You can't read the database without pushing your changes first or rolling back");
		}
		AntidoteInnerCounter counter;
		if (getType() == AntidoteType.GMapType){
			LowLevelGMap lowGMap = new LowLevelGMap(getName(), getBucket(), getClient());
			AntidoteOuterGMap outerMap = lowGMap.createAntidoteGMap();
			if (getPath().size() == 1){
				counter = outerMap.getCounterEntry(getPath().get(0).getKey().toStringUtf8());
			}
			else{
				counter = readDatabaseHelper(getPath(), outerMap).getCounterEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
			}		
			value = counter.getValue();
		}
		else if (getType() == AntidoteType.AWMapType){ 
			LowLevelAWMap lowAWMap = new LowLevelAWMap(getName(), getBucket(), getClient());
			AntidoteOuterAWMap outerMap = lowAWMap.createAntidoteAWMap();
			if (getPath().size() == 1){
				counter = outerMap.getCounterEntry(getPath().get(0).getKey().toStringUtf8());
			}
			else{
				counter = readDatabaseHelper(getPath(), outerMap).getCounterEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
			}		
			value = counter.getValue();
		}
	}
	
	/**
	 * Gets the value.
	 *
	 * @return the value
	 */
	public int getValue(){
		return value;
	}
	
	/**
	 * Increment the value by one.
	 */
	public void increment(){
		increment(1);
	}
	
	/**
	 * Increment the value.
	 *
	 * @param inc the increment by which the value is incremented
	 */
	public void increment(int inc){
		incrementLocal(inc);
		List<AntidoteMapUpdate> counterIncrement = new ArrayList<AntidoteMapUpdate>(); 
		counterIncrement.add(getClient().createCounterIncrement(inc));
		updateHelper(counterIncrement);
	}
	
	/**
	 * Execute increment locally.
	 *
	 * @param inc the increment
	 */
	protected void incrementLocal(int inc){
		value = value + inc;
	}
}
