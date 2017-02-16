package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

import interfaces.MVRegisterCRDT;

/**
 * The Class AntidoteInnerMVRegister.
 */
public final class AntidoteInnerMVRegister extends AntidoteInnerCRDT implements MVRegisterCRDT{

/** The value list. */
private List<String> valueList;
	
	/**
	 * Instantiates a new antidote map MV register entry.
	 *
	 * @param valueList the value list
	 * @param antidoteClient the antidote client
	 * @param name the name
	 * @param bucket the bucket
	 * @param path the path
	 * @param outerMapType the outer map type
	 */
	public AntidoteInnerMVRegister(List<String> valueList, AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path, CRDT_type outerMapType){
		super(antidoteClient, name, bucket, path, outerMapType);
		this.valueList = valueList;
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.MVRegisterInterface#rollBack()
	 */
	public void rollBack(){
		clearUpdateList();
		readDatabase();
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.MVRegisterInterface#synchronize()
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
		AntidoteInnerMVRegister mvRegister;
		if (getType() == AntidoteType.GMapType){
			GMapRef lowGMap = new GMapRef(getName(), getBucket(), getClient());
			AntidoteOuterGMap outerMap = lowGMap.createAntidoteGMap();
			if (getPath().size() == 1){
				mvRegister = outerMap.getMVRegisterEntry(getPath().get(0).getKey().toStringUtf8());
			}
			else{
				mvRegister = readDatabaseHelper(getPath(), outerMap).getMVRegisterEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
			}		
			valueList = new ArrayList<>(mvRegister.getValueList());
		}
		else if (getType() == AntidoteType.AWMapType){ 
			AWMapRef lowAWMap = new AWMapRef(getName(), getBucket(), getClient());
			AntidoteOuterAWMap outerMap = lowAWMap.createAntidoteAWMap();
			if (getPath().size() == 1){
				mvRegister = outerMap.getMVRegisterEntry(getPath().get(0).getKey().toStringUtf8());
			}
			else{
				mvRegister = readDatabaseHelper(getPath(), outerMap).getMVRegisterEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
			}		
			valueList = new ArrayList<>(mvRegister.getValueList());
		}
	}
	
	/**
	 * Gets the value list.
	 *
	 * @return the value list
	 */
	public List<String> getValueList(){
		return Collections.unmodifiableList(valueList);
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.MVRegisterInterface#getValueListBS()
	 */
	public List<ByteString> getValueListBS(){
		List<ByteString> valueListBS = new ArrayList<>();
		for (String value : valueList){
			valueListBS.add(ByteString.copyFromUtf8(value));
		}
		return valueListBS;
	}
	
	/**
	 * Locally set the register to a new value.
	 *
	 * @param value the value
	 */
	public void setLocal(String value){
		valueList.clear();
		valueList.add(value);
	}
		
	/**
	 * Set the register to a new value.
	 *
	 * @param value the value
	 */
	public void setValue(String value){
		setLocal(value);
		List<AntidoteMapUpdate> registerSet = new ArrayList<AntidoteMapUpdate>(); 
		registerSet.add(AntidoteMapUpdate.createMVRegisterSet(value));
		updateHelper(registerSet);
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.MVRegisterInterface#setValue(com.google.protobuf.ByteString)
	 */
	public void setValueBS(ByteString value){
		setLocal(value.toStringUtf8());
		List<AntidoteMapUpdate> registerSet = new ArrayList<AntidoteMapUpdate>(); 
		registerSet.add(AntidoteMapUpdate.createMVRegisterSet(value.toStringUtf8()));
		updateHelper(registerSet);
	}
}
