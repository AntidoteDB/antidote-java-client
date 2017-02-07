package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

/**
 * The Class AntidoteInnerLWWRegister.
 */
public class AntidoteInnerLWWRegister extends AntidoteInnerObject implements InterfaceLWWRegister{
	
	/** The value. */
	private String value;
	
	/**
	 * Instantiates a new antidote map register entry.
	 *
	 * @param value the value
	 * @param antidoteClient the antidote client
	 * @param name the name
	 * @param bucket the bucket
	 * @param path the path
	 * @param outerMapType the outer map type
	 */
	public AntidoteInnerLWWRegister(String value, AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path, CRDT_type outerMapType){
		super(antidoteClient, name, bucket, path, outerMapType);
		this.value = value;
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.RegisterInterface#rollBack()
	 */
	public void rollBack(){
		clearUpdateList();
		readDatabase();
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.RegisterInterface#synchronize()
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
		AntidoteInnerLWWRegister register;
		if (getType() == AntidoteType.GMapType){
			LowLevelGMap lowGMap = new LowLevelGMap(getName(), getBucket(), getClient());
			AntidoteOuterGMap outerMap = lowGMap.createAntidoteGMap();
			if (getPath().size() == 1){
				register = outerMap.getLWWRegisterEntry(getPath().get(0).getKey().toStringUtf8());
			}
			else{
				register = readDatabaseHelper(getPath(), outerMap).getLWWRegisterEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
			}		
			value = register.getValue();
		}
		else if (getType() == AntidoteType.AWMapType){ 
			LowLevelAWMap lowAWMap = new LowLevelAWMap(getName(), getBucket(), getClient());
			AntidoteOuterAWMap outerMap = lowAWMap.createAntidoteAWMap();
			if (getPath().size() == 1){
				register = outerMap.getLWWRegisterEntry(getPath().get(0).getKey().toStringUtf8());
			}
			else{
				register = readDatabaseHelper(getPath(), outerMap).getLWWRegisterEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
			}		
			value = register.getValue();
		}
	}
	
	/**
	 * Gets the value.
	 *
	 * @return the value
	 */
	public String getValue(){
		return value;
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.RegisterInterface#getValueBS()
	 */
	public ByteString getValueBS(){
		return ByteString.copyFromUtf8(value);
	}
	
	/**
	 * Locally set the register to a new value.
	 *
	 * @param value the value
	 */
	protected void setLocal(String value){
		this.value = value;
	}

	/**
	 * Set the register to a new value.
	 *
	 * @param value the value
	 */
	public void setValue(String value){
		setLocal(value);
		List<AntidoteMapUpdate> registerSet = new ArrayList<AntidoteMapUpdate>(); 
		registerSet.add(getClient().createRegisterSet(value));
		updateHelper(registerSet);
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.RegisterInterface#setValue(com.google.protobuf.ByteString)
	 */
	public void setValueBS(ByteString value){
		setLocal(value.toStringUtf8());
		List<AntidoteMapUpdate> registerSet = new ArrayList<AntidoteMapUpdate>(); 
		registerSet.add(getClient().createRegisterSet(value.toStringUtf8()));
		updateHelper(registerSet);
	}
}
