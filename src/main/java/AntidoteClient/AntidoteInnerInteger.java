package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;

/**
 * The Class AntidoteInnerInteger.
 */
public class AntidoteInnerInteger extends AntidoteInnerObject implements InterfaceInteger{
	
	/** The value. */
	private int value;
	
	/**
	 * Instantiates a new antidote map integer entry.
	 *
	 * @param value the value
	 * @param antidoteClient the antidote client
	 * @param name the name
	 * @param bucket the bucket
	 * @param path the path
	 * @param outerMapType the outer map type
	 */
	public AntidoteInnerInteger(int value, AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path, CRDT_type outerMapType){
		super(antidoteClient, name, bucket, path, outerMapType);
		this.value = value;
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.IntegerInterface#rollBack()
	 */
	public void rollBack(){
		clearUpdateList();
		readDatabase();
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.IntegerInterface#synchronize()
	 */
	public void synchronize(){
		push();
		readDatabase();
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
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){
		if (getUpdateList().size() > 0){
			throw new AntidoteException("You can't read the database without pushing your changes first or rolling back");
		}
		AntidoteInnerInteger integer;
		if (getType() == AntidoteType.GMapType){
			LowLevelGMap lowGMap = new LowLevelGMap(getName(), getBucket(), getClient());
			AntidoteOuterGMap outerMap = lowGMap.createAntidoteGMap();
			if (getPath().size() == 1){
				integer = outerMap.getIntegerEntry(getPath().get(0).getKey().toStringUtf8());
			}
			else{
				integer = readDatabaseHelper(getPath(), outerMap).getIntegerEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
			}		
			value = integer.getValue();
		}
		else if (getType() == AntidoteType.AWMapType){ 
			LowLevelAWMap lowAWMap = new LowLevelAWMap(getName(), getBucket(), getClient());
			AntidoteOuterAWMap outerMap = lowAWMap.createAntidoteAWMap();
			if (getPath().size() == 1){
				integer = outerMap.getIntegerEntry(getPath().get(0).getKey().toStringUtf8());
			}
			else{
				integer = readDatabaseHelper(getPath(), outerMap).getIntegerEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
			}		
			value = integer.getValue();
		}
	}
	
	/**
	 * Execute increment locally.
	 *
	 * @param inc the increment
	 */
	protected void incrementLocal(int inc){
		value = value + inc;
	}
	
	/**
	 * Increment by one.
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
		List<AntidoteMapUpdate> integerIncrement = new ArrayList<AntidoteMapUpdate>(); 
		integerIncrement.add(getClient().createIntegerIncrement(inc));
		updateHelper(integerIncrement);
	}
	
	/**
	 * Sets the integer to a new value locally.
	 *
	 * @param value the new value
	 */
	protected void setLocal(int value){
		this.value = value;
	}
	
	/**
	 * Sets the integer to a new value.
	 *
	 * @param value the new value
	 */
	public void setValue(int value){
		setLocal(value);
		List<AntidoteMapUpdate> integerSet = new ArrayList<AntidoteMapUpdate>(); 
		integerSet.add(getClient().createIntegerSet(value));
		updateHelper(integerSet);
	}
}
