package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.protobuf.ByteString;

import interfaces.MVRegisterCRDT;

/**
 * The Class AntidoteOuterMVRegister.
 */
public final class AntidoteOuterMVRegister extends AntidoteCRDT implements MVRegisterCRDT{
	
	/** The value list. */
	private List<String> valueList;

	/** The low level register. */
	private final MVRegisterRef lowLevelRegister;
	
	/**
	 * Instantiates a new antidote MV register.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param valueList the values of the MV-Register
	 * @param antidoteClient the antidote client
	 */
	public AntidoteOuterMVRegister(String name, String bucket, List<String> valueList, AntidoteClient antidoteClient) {
		super(name, bucket, antidoteClient, AntidoteType.MVRegisterType);
		this.valueList = valueList;
		lowLevelRegister = new MVRegisterRef(name, bucket, antidoteClient);
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
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){
		if (getUpdateList().size() > 0){
			throw new AntidoteException("You can't read the database without pushing your changes first or rolling back");
		}
		valueList = lowLevelRegister.readRegisterValues();
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
	 * Update the MV-Register.
	 *
	 * @param element the element
	 */
	public void setValue(String element){
		valueList.clear();
		valueList.add(element);
		updateAdd(lowLevelRegister.setOpBuilder(ByteString.copyFromUtf8(element)));
	}

	public void setValue(String element, AntidoteTransaction antidoteTransaction){
		valueList.clear();
		valueList.add(element);
		antidoteTransaction.updateHelper(lowLevelRegister.setOpBuilder(ByteString.copyFromUtf8(element)),getName(),getBucket(),getType());
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.MVRegisterInterface#setValue(com.google.protobuf.ByteString)
	 */
	public void setValueBS(ByteString element){
		valueList.clear();
		valueList.add(element.toStringUtf8());
		updateAdd(lowLevelRegister.setOpBuilder(element));
	}

	public void setValueBS(ByteString element, AntidoteTransaction antidoteTransaction){
		valueList.clear();
		valueList.add(element.toStringUtf8());
		antidoteTransaction.updateHelper(lowLevelRegister.setOpBuilder(element),getName(),getBucket(),getType());
	}
}
