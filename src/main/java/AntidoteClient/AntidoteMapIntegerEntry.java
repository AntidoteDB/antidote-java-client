package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;

/**
 * The Class AntidoteMapIntegerEntry.
 */
public class AntidoteMapIntegerEntry extends AntidoteMapEntry {
	
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
	public AntidoteMapIntegerEntry(int value, AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path, CRDT_type outerMapType){
		super(antidoteClient, name, bucket, path, outerMapType);
		this.value = value;
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
		AntidoteMapIntegerEntry integer;
		if (getOuterMapType() == CRDT_type.GMAP){
			AntidoteGMap outerMap = getClient().readGMap(getName(), getBucket());
			if (getPath().size() == 1){
				integer = outerMap.getIntegerEntry(getPath().get(0).getKey().toStringUtf8());
			}
			else{
				AntidoteMapMapEntry innerMap = outerMap.getAWMapEntry(getPath().get(0).getKey().toStringUtf8());
				for (int i = 1; i<getPath().size()-1; i++){
					if (getPath().get(i).getType()==CRDT_type.AWMAP){
						innerMap = innerMap.getAWMapEntry(getPath().get(i).getKey().toStringUtf8());
					}
					else if (getPath().get(i).getType()==CRDT_type.GMAP){
						innerMap = innerMap.getGMapEntry(getPath().get(i).getKey().toStringUtf8());

					}
				}
				integer = innerMap.getIntegerEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
			}		
			value = integer.getValue();
		}
		else if (getOuterMapType() == CRDT_type.AWMAP){ 
			AntidoteAWMap outerMap = getClient().readAWMap(getName(), getBucket());
			if (getPath().size() == 1){
				integer = outerMap.getIntegerEntry(getPath().get(0).getKey().toStringUtf8());
			}
			else{
				AntidoteMapMapEntry innerMap = outerMap.getAWMapEntry(getPath().get(0).getKey().toStringUtf8());
				for (int i = 1; i<getPath().size()-1; i++){
					if (getPath().get(i).getType()==CRDT_type.AWMAP){
						innerMap = innerMap.getAWMapEntry(getPath().get(i).getKey().toStringUtf8());
					}
					else if (getPath().get(i).getType()==CRDT_type.GMAP){
						innerMap = innerMap.getGMapEntry(getPath().get(i).getKey().toStringUtf8());
					}
				}
				integer = innerMap.getIntegerEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
			}		
			value = integer.getValue();
		}
	}
	
	/**
	 * Execute increment locally
	 *
	 * @param inc the increment
	 */
	public void incrementLocal(int inc){
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
	public void setLocal(int value){
		this.value = value;
	}
	
	/**
	 * Sets the integer to a new value.
	 *
	 * @param value the new value
	 */
	public void set(int value){
		setLocal(value);
		List<AntidoteMapUpdate> integerSet = new ArrayList<AntidoteMapUpdate>(); 
		integerSet.add(getClient().createIntegerSet(value));
		updateHelper(integerSet);
	}
}
