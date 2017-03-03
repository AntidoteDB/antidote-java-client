package main.java.AntidoteClient;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import interfaces.CounterCRDT;
import java.util.ArrayList;
import java.util.List;

/**
 * The Class AntidoteInnerCounter.
 */
public final class AntidoteInnerCounter extends AntidoteInnerCRDT implements CounterCRDT{
	
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
	
	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(AntidoteTransaction antidoteTransaction){
		AntidoteInnerCounter counter;
		if (getType() == AntidoteType.GMapType){
			GMapRef lowGMap = new GMapRef(getName(), getBucket(), getClient());
			AntidoteOuterGMap outerMap = lowGMap.createAntidoteGMap(antidoteTransaction);
			if (getPath().size() == 1){
				counter = outerMap.getCounterEntry(getPath().get(0).getKey().toStringUtf8());
			}
			else{
				counter = readDatabaseHelper(getPath(), outerMap).getCounterEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
			}		
			value = counter.getValue();
		}
		else if (getType() == AntidoteType.AWMapType){ 
			AWMapRef lowAWMap = new AWMapRef(getName(), getBucket(), getClient());
			AntidoteOuterAWMap outerMap = lowAWMap.createAntidoteAWMap(antidoteTransaction);
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
	 *
	 * @param antidoteTransaction the antidote transaction
	 */
	public void increment(AntidoteTransaction antidoteTransaction){
		increment(1, antidoteTransaction);
	}

	/**
	 * Increment the value.
	 *
	 * @param inc the increment by which the value is incremented
	 * @param antidoteTransaction the antidote transaction
	 */
	public void increment(int inc, AntidoteTransaction antidoteTransaction){
		incrementLocal(inc);
		List<AntidoteMapUpdate> counterIncrement = new ArrayList<AntidoteMapUpdate>();
		counterIncrement.add(AntidoteMapUpdate.createCounterIncrement(inc));
		updateHelper(counterIncrement, antidoteTransaction);
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
