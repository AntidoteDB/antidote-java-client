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
	
	/** The list of locally executed but not yet pushed operations. */
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
		if (updateList.size() > 0){
			throw new AntidoteException("You can't read the database without pushing your changes first or rolling back");
		}
		valueList = getClient().readMVRegister(getName(), getBucket()).getValueList();
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.MVRegisterInterface#rollBack()
	 */
	public void rollBack(){
		updateList.clear();
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
		updateList.add(element);
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.MVRegisterInterface#setValue(com.google.protobuf.ByteString)
	 */
	public void setValueBS(ByteString element){
		valueList.clear();
		valueList.add(element.toStringUtf8());
		updateList.add(element.toStringUtf8());
	}
	
	/**
	 * Push locally executed updates to database. Uses a transaction.
	 */
	public void push(){
		AntidoteTransaction antidoteTransaction = new AntidoteTransaction(getClient());  
		ByteString descriptor = antidoteTransaction.startTransaction();
		for(String update : updateList){
			antidoteTransaction.updateMVRegisterTransaction(getName(), getBucket(), update, descriptor);
		}
		antidoteTransaction.commitTransaction(descriptor);
		updateList.clear();
	}
}
