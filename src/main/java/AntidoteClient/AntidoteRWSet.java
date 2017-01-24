package main.java.AntidoteClient;

import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;

/**
 * The Class AntidoteRWSet.
 */
public class AntidoteRWSet extends AntidoteSet implements SetInterface{
	
	/**
	 * Instantiates a new antidote RW set.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param valueList the value list
	 * @param antidoteClient the antidote client
	 */
	public AntidoteRWSet(String name, String bucket, List<String> valueList, AntidoteClient antidoteClient){
		super(name, bucket, valueList, antidoteClient);
	}
	
	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){
		if (getUpdateList().size() > 0){
			throw new AntidoteException("You can't read the database without pushing your changes first or rolling back");
		}
		setValueList(getClient().readRWSet(getName(), getBucket()).getValueList());
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.SetInterface#rollBack()
	 */
	public void rollBack(){
		clearUpdateList();
		readDatabase();
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.SetInterface#synchronize()
	 */
	public void synchronize(){
		push();
		readDatabase();
	}
	
	/**
	 * Push locally executed updates to database. Uses a transaction.
	 */
	public void push(){
		AntidoteTransaction antidoteTransaction = new AntidoteTransaction(getClient());  
		ByteString descriptor = antidoteTransaction.startTransaction();
		for(Map.Entry<Integer, List<String>> update : getUpdateList()){
			if(update.getKey() == 1){
				antidoteTransaction.addRWSetElementTransaction(getName(), getBucket(), update.getValue(), descriptor);
			}
			else if(update.getKey() == 2){
				antidoteTransaction.removeRWSetElementTransaction(getName(), getBucket(), update.getValue(), descriptor);
			}
		}
		antidoteTransaction.commitTransaction(descriptor);
		clearUpdateList();
	}
}
