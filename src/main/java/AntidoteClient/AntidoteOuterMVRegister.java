package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import com.google.protobuf.ByteString;
import interfaces.MVRegisterCRDT;

/**
 * The Class AntidoteOuterMVRegister.
 */
public final class AntidoteOuterMVRegister extends AntidoteCRDT implements MVRegisterCRDT{
	
	/** The value list. */
	private List<ByteString> valueList;

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
	public AntidoteOuterMVRegister(String name, String bucket, List<ByteString> valueList, AntidoteClient antidoteClient) {
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
		List<String> valueListString = new ArrayList<>();
		for (ByteString value : valueList){
			valueListString.add(value.toStringUtf8());
		}
		return new ArrayList<String>(valueListString);
	}

	protected void readValueList(List<ByteString> newValueList){
		valueList = new ArrayList<ByteString>(newValueList);
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.MVRegisterInterface#getValueListBS()
	 */
	public List<ByteString> getValueListBS(){
		return new ArrayList<ByteString>(valueList);
	}
	
	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(AntidoteTransaction antidoteTransaction){
		valueList = new ArrayList<ByteString>(lowLevelRegister.readRegisterValuesBS(antidoteTransaction));
	}

	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){
		valueList = new ArrayList<ByteString>(lowLevelRegister.readRegisterValuesBS());
	}

	/**
	 * Update the MV-Register.
	 *
	 * @param element the element
	 * @param antidoteTransaction the antidote transaction
	 */
	public void setValue(String element, AntidoteTransaction antidoteTransaction){
		valueList = new ArrayList<>();
		valueList.add(ByteString.copyFromUtf8(element));
		antidoteTransaction.updateHelper(lowLevelRegister.setOpBuilder(ByteString.copyFromUtf8(element)),getName(),getBucket(),getType());
	}

	public void setValueBS(ByteString element, AntidoteTransaction antidoteTransaction){
		valueList = new ArrayList<>();
		valueList.add(element);
		antidoteTransaction.updateHelper(lowLevelRegister.setOpBuilder(element),getName(),getBucket(),getType());
	}
}
