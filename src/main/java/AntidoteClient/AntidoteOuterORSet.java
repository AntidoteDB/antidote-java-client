package main.java.AntidoteClient;

import java.util.List;

import interfaces.SetCRDT;

/**
 * The Class AntidoteOuterORSet.
 */
public final class AntidoteOuterORSet extends AntidoteOuterSet implements SetCRDT{
	
	/** The low level set. */
	private final ORSetRef lowLevelSet;
	
	/**
	 * Instantiates a new antidote OR set.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param valueList the value list
	 * @param antidoteClient the antidote client
	 */
	public AntidoteOuterORSet(String name, String bucket, List<String> valueList, AntidoteClient antidoteClient){
		super(name, bucket, valueList, antidoteClient, AntidoteType.ORSetType);
		lowLevelSet = new ORSetRef(name, bucket, antidoteClient);
	}
	
	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){
		if (getUpdateList().size() > 0){
			throw new AntidoteException("You can't read the database without pushing your changes first or rolling back");
		}
		setValues(lowLevelSet.readValueList());
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
}
