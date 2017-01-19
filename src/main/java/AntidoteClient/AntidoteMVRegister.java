package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;

/**
 * The Class AntidoteMVRegister.
 */
public class AntidoteMVRegister extends AntidoteObject implements MVRegisterInterface{
	
	/** The value list. */
	private List<String> valueList;
	
	/** The list of locally but not yet pushed operations. */
	private List<String> updateList;

	
	/**
	 * Instantiates a new antidote MV register.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param valueList the values of the MV-Register
	 * @param antidoteClient the antidote client
	 */
	public AntidoteMVRegister(String name, String bucket, List<String> valueList, AntidoteClient antidoteClient) {
		super(name, bucket, antidoteClient);
		this.valueList = valueList;
		updateList = new ArrayList<>();
	}
	
	/**
	 * Gets the value list.
	 *
	 * @return the value list
	 */
	public List<String> getValueList(){
		return valueList;
	}
	
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
		if (updateList.size() > 0){
			throw new AntidoteException("You can't read the database without pushing your changes first or rolling back");
		}
		valueList = getClient().readMVRegister(getName(), getBucket()).getValueList();
	}
	
	public void rollBack(){
		updateList.clear();
		readDatabase();
	}
	
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
		updateList.add(element);
	}
	
	public void setValue(ByteString element){
		valueList.clear();
		valueList.add(element.toStringUtf8());
		updateList.add(element.toStringUtf8());
	}
	
	/**
	 * Push locally executed updates to database.
	 */
	public void push(){
		for(String update : updateList){
			getClient().updateMVRegister(getName(), getBucket(), update);	
		}
		updateList.clear();
	}
}
