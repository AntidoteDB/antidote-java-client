package main.java.AntidoteClient;

import com.google.protobuf.ByteString;

/**
 * The Class AntidoteOuterLWWRegister.
 */
public class AntidoteOuterLWWRegister extends AntidoteObject implements InterfaceLWWRegister{
	
	/** The value. */
	private String value;

	/** The low level register. */
	private LowLevelLWWRegister lowLevelRegister;

	/**
	 * Instantiates a new antidote register.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param value the value of the register
	 * @param antidoteClient the antidote client
	 */
	public AntidoteOuterLWWRegister(String name, String bucket, String value, AntidoteClient antidoteClient) {
		super(name, bucket, antidoteClient, AntidoteType.LWWRegisterType);
		this.value = value;
		lowLevelRegister = new LowLevelLWWRegister(name, bucket, antidoteClient);
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
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){
		if (getUpdateList().size() > 0){
			throw new AntidoteException("You can't read the database without pushing your changes first or rolling back");
		}
		value = lowLevelRegister.readRegisterValue();
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
	 * Set the value of the register.
	 *
	 * @param element the element
	 */
	public void setValue(String element){
		value = element;
		updateAdd(lowLevelRegister.setOpBuilder(ByteString.copyFromUtf8(element)));
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.RegisterInterface#setValue(com.google.protobuf.ByteString)
	 */
	public void setValueBS(ByteString element){
		value = element.toStringUtf8();
		updateAdd(lowLevelRegister.setOpBuilder(element));
	}
}
