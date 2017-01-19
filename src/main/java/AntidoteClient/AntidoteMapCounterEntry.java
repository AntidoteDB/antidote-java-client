package main.java.AntidoteClient;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;

import java.util.ArrayList;
import java.util.List;

/**
 * The Class AntidoteMapCounterEntry.
 */
public class AntidoteMapCounterEntry extends AntidoteMapEntry implements CounterInterface{
	
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
	public AntidoteMapCounterEntry(int value, AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path, CRDT_type outerMapType){
		super(antidoteClient, name, bucket, path, outerMapType);
		this.value = value;
	}
	
	public void rollBack(){
		clearUpdateList();
		readDatabase();
	}
	
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
		AntidoteMapCounterEntry counter;
		if (getOuterMapType() == CRDT_type.GMAP){
			AntidoteGMap outerMap = getClient().readGMap(getName(), getBucket());
			if (getPath().size() == 1){
				counter = outerMap.getCounterEntry(getPath().get(0).getKey().toStringUtf8());
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
				counter = innerMap.getCounterEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
			}		
			value = counter.getValue();
		}
		else if (getOuterMapType() == CRDT_type.AWMAP){ 
			AntidoteAWMap outerMap = getClient().readAWMap(getName(), getBucket());
			if (getPath().size() == 1){
				counter = outerMap.getCounterEntry(getPath().get(0).getKey().toStringUtf8());
			}
			else{
				AntidoteMapMapEntry innerMap = outerMap.getAWMapEntry(getPath().get(0).getKey().toStringUtf8());
				for (int i = 1; i<getPath().size()-1; i++){ // hei hunn ech op 1 gesaat
					if (getPath().get(i).getType()==CRDT_type.AWMAP){
						innerMap = innerMap.getAWMapEntry(getPath().get(i).getKey().toStringUtf8());
					}
					else if (getPath().get(i).getType()==CRDT_type.GMAP){
						innerMap = innerMap.getGMapEntry(getPath().get(i).getKey().toStringUtf8());
					}
				}
				System.out.println(innerMap);
				counter = innerMap.getCounterEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
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
	 * Execute increment locally
	 *
	 * @param inc the increment
	 */
	public void incrementLocal(int inc){
		value = value + inc;
	}
}
