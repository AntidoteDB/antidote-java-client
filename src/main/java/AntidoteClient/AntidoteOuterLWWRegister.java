package main.java.AntidoteClient;

import com.google.protobuf.ByteString;
import interfaces.LWWRegisterCRDT;

/**
 * The Class AntidoteOuterLWWRegister.
 */
public final class AntidoteOuterLWWRegister extends AntidoteCRDT implements LWWRegisterCRDT{
	
	/** The value. */
	private ByteString value;

	/** The low level register. */
	private final LWWRegisterRef lowLevelRegister;

	/**
	 * Instantiates a new antidote register.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param value the value of the register
	 * @param antidoteClient the antidote client
	 */
	public AntidoteOuterLWWRegister(String name, String bucket, ByteString value, AntidoteClient antidoteClient) {
		super(name, bucket, antidoteClient, AntidoteType.LWWRegisterType);
		this.value = value;
		lowLevelRegister = new LWWRegisterRef(name, bucket, antidoteClient);
	}
	
	/**
	 * Gets the value.
	 *
	 * @return the value
	 */
	public String getValue(){
		return value.toStringUtf8();
	}

	protected void readValueList(ByteString newValueList){
		value = newValueList;
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.RegisterInterface#getValueBS()
	 */
	public ByteString getValueBS(){
		return value;
	}
	
	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(AntidoteTransaction antidoteTransaction){
		value = lowLevelRegister.readRegisterValueBS(antidoteTransaction);
	}

	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){
		value = lowLevelRegister.readRegisterValueBS();
	}
	
	/**
	 * Set the value of the register.
	 *
	 * @param element the element
	 * @param antidoteTransaction the antidote transaction
	 */
	public void setValue(String element, AntidoteTransaction antidoteTransaction){
		value = ByteString.copyFromUtf8(element);
		antidoteTransaction.updateHelper(lowLevelRegister.setOpBuilder(ByteString.copyFromUtf8(element)),getName(),getBucket(),getType());
	}

	public void setValueBS(ByteString element, AntidoteTransaction antidoteTransaction){
		value = element;
		antidoteTransaction.updateHelper(lowLevelRegister.setOpBuilder(element),getName(),getBucket(),getType());
	}
}
