package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;

/**
 * The Class AntidoteRegister.
 */
public class AntidoteRegister extends AntidoteObject implements RegisterInterface{
	
	/** The value. */
	private String value;
	
	/** The update list. */
	private List<String> updateList;

	/**
	 * Instantiates a new antidote register.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param value the value of the register
	 * @param antidoteClient the antidote client
	 */
	public AntidoteRegister(String name, String bucket, String value, AntidoteClient antidoteClient) {
		super(name, bucket, antidoteClient);
		this.value = value;
		updateList = new ArrayList<>();
	}
	
	/**
	 * Gets the value.
	 *
	 * @return the value
	 */
	public String getValue(){
		return value;
	}
	
	public ByteString getValueBS(){
		return ByteString.copyFromUtf8(value);
	}
	
	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){
		if (updateList.size() > 0){
			throw new AntidoteException("You can't read the database without pushing your changes first or rolling back");
		}
		value = getClient().readRegister(getName(), getBucket()).getValue();
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
	 * Set the value of the register.
	 *
	 * @param element the element
	 */
	public void setValue(String element){
		value = element;
		updateList.add(element);
	}
	
	public void setValue(ByteString element){
		value = element.toStringUtf8();
		updateList.add(element.toStringUtf8());
	}
	
	/**
	 * Push locally executed updates to database.
	 */
	public void push(){
		for(String update : updateList){
			getClient().updateRegister(getName(), getBucket(), update);	
		}
		updateList.clear();
	}
}
