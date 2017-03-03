package main.java.AntidoteClient;

import java.util.List;

import com.google.protobuf.ByteString;

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
	public AntidoteOuterORSet(String name, String bucket, List<ByteString> valueList, AntidoteClient antidoteClient){
		super(name, bucket, valueList, antidoteClient, AntidoteType.ORSetType);
		lowLevelSet = new ORSetRef(name, bucket, antidoteClient);
	}
	
	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(AntidoteTransaction antidoteTransaction){
		setValues(lowLevelSet.readValueListBS(antidoteTransaction));
	}
}
