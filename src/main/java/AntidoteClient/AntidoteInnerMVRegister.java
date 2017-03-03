package main.java.AntidoteClient;

import java.util.ArrayList;
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
private List<ByteString> valueList;
	
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
	public AntidoteInnerMVRegister(List<ByteString> valueList, AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path, CRDT_type outerMapType){
		super(antidoteClient, name, bucket, path, outerMapType);
		this.valueList = valueList;
	}
	
	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(AntidoteTransaction antidoteTransaction){	
		AntidoteInnerMVRegister mvRegister;
		if (getType() == AntidoteType.GMapType){
			GMapRef lowGMap = new GMapRef(getName(), getBucket(), getClient());
			AntidoteOuterGMap outerMap = lowGMap.createAntidoteGMap(antidoteTransaction);
			if (getPath().size() == 1){
				mvRegister = outerMap.getMVRegisterEntry(getPath().get(0).getKey().toStringUtf8());
			}
			else{
				mvRegister = readDatabaseHelper(getPath(), outerMap).getMVRegisterEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
			}		
			valueList = new ArrayList<>(mvRegister.getValueListBS());
		}
		else if (getType() == AntidoteType.AWMapType){ 
			AWMapRef lowAWMap = new AWMapRef(getName(), getBucket(), getClient());
			AntidoteOuterAWMap outerMap = lowAWMap.createAntidoteAWMap(antidoteTransaction);
			if (getPath().size() == 1){
				mvRegister = outerMap.getMVRegisterEntry(getPath().get(0).getKey().toStringUtf8());
			}
			else{
				mvRegister = readDatabaseHelper(getPath(), outerMap).getMVRegisterEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
			}		
			valueList = new ArrayList<>(mvRegister.getValueListBS());
		}
	}
	
	/**
	 * Gets the value list.
	 *
	 * @return the value list
	 */
	public List<String> getValueList(){
		List<String> valueListString = new ArrayList<>();
		for (ByteString value : valueList){
			valueListString.add(value.toStringUtf8());
		}
		return new ArrayList<>(valueListString);
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.MVRegisterInterface#getValueListBS()
	 */
	public List<ByteString> getValueListBS(){
		return new ArrayList<>(valueList);
	}
	
	/**
	 * Locally set the register to a new value.
	 *
	 * @param value the value
	 */
	public void setLocal(ByteString value){
		valueList = new ArrayList<>();
		valueList.add(value);
	}
	
	/**
	 * Set the register to a new value.
	 *
	 * @param value the value
	 * @param antidoteTransaction the antidote transaction
	 */
	public void setValue(String value, AntidoteTransaction antidoteTransaction){
		setLocal(ByteString.copyFromUtf8(value));
		List<AntidoteMapUpdate> registerSet = new ArrayList<AntidoteMapUpdate>();
		registerSet.add(AntidoteMapUpdate.createMVRegisterSet(value));
		updateHelper(registerSet, antidoteTransaction);
	}

	public void setValueBS(ByteString value, AntidoteTransaction antidoteTransaction){
		setLocal(value);
		List<AntidoteMapUpdate> registerSet = new ArrayList<AntidoteMapUpdate>();
		registerSet.add(AntidoteMapUpdate.createMVRegisterSet(value.toStringUtf8()));
		updateHelper(registerSet, antidoteTransaction);
	}
}
