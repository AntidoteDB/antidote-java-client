package main.java.AntidoteClient;

import java.util.List;

/**
 * The Class AntidoteOuterORSet.
 */
public class AntidoteOuterORSet extends AntidoteOuterSet implements InterfaceSet{
	
	/** The low level set. */
	private LowLevelORSet lowLevelSet;
	
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
		lowLevelSet = new LowLevelORSet(name, bucket, antidoteClient);
	}
	
	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){
		if (getUpdateList().size() > 0){
			throw new AntidoteException("You can't read the database without pushing your changes first or rolling back");
		}
		setValueList(lowLevelSet.readValueList());
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
