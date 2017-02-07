package main.java.AntidoteClient;

import java.util.List;

/**
 * The Class AntidoteOuterRWSet.
 */
public class AntidoteOuterRWSet extends AntidoteOuterSet implements InterfaceSet{
	
	/** The low level set. */
	private LowLevelRWSet lowLevelSet;
	
	/**
	 * Instantiates a new antidote RW set.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param valueList the value list
	 * @param antidoteClient the antidote client
	 */
	public AntidoteOuterRWSet(String name, String bucket, List<String> valueList, AntidoteClient antidoteClient){
		super(name, bucket, valueList, antidoteClient, AntidoteType.RWSetType);
		lowLevelSet = new LowLevelRWSet(name, bucket, antidoteClient);
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
