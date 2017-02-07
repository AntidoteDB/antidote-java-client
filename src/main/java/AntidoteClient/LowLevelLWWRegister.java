package main.java.AntidoteClient;

import com.basho.riak.protobuf.AntidotePB.ApbGetRegResp;
import com.google.protobuf.ByteString;

/**
 * The Class LowLevelLWWRegister.
 */
public class LowLevelLWWRegister extends LowLevelRegister{
	
	/**
	 * Instantiates a new low level LWW register.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param antidoteClient the antidote client
	 */
	public LowLevelLWWRegister(String name, String bucket, AntidoteClient antidoteClient){
		super(name, bucket, antidoteClient);
	}
	
	/**
	 * Sets the value.
	 *
	 * @param value the value
	 */
	public void set(String value){
		super.set(value, AntidoteType.LWWRegisterType);
	}
	
	/**
	 * Sets the value.
	 *
	 * @param value the new value
	 */
	public void setBS(ByteString value){
		super.setBS(value, AntidoteType.LWWRegisterType);
	}
	
	/**
	 * Sets the value.
	 *
	 * @param value the value
	 * @param antidoteTransaction the antidote transaction
	 */
	public void setBS(ByteString value, AntidoteTransaction antidoteTransaction){
		super.setBS(value, AntidoteType.LWWRegisterType, antidoteTransaction);
	}
	
	/**
	 * Sets the value.
	 *
	 * @param value the value
	 * @param antidoteTransaction the antidote transaction
	 */
	public void set(String value, AntidoteTransaction antidoteTransaction){
		super.set(value, AntidoteType.LWWRegisterType, antidoteTransaction);
	}
	
	/**
     * Read register from database.
     *
     * @return the antidote register
     */
    public AntidoteOuterLWWRegister createAntidoteLWWRegister() {
        ApbGetRegResp reg = getClient().readHelper(getName(), getBucket(), AntidoteType.LWWRegisterType).getObjects().getObjects(0).getReg();
        return new AntidoteOuterLWWRegister(getName(), getBucket(), reg.getValue().toStringUtf8(), getClient()); 
    }
    
    /**
     * Read register from database.
     *
     * @param antidoteTransaction the antidote transaction
     * @return the antidote register
     */
    public AntidoteOuterLWWRegister createAntidoteLWWRegister(AntidoteTransaction antidoteTransaction){
    	ApbGetRegResp reg = antidoteTransaction.readHelper(getName(), getBucket(), AntidoteType.LWWRegisterType).getObjects(0).getReg();
        return new AntidoteOuterLWWRegister(getName(), getBucket(), reg.getValue().toStringUtf8(), getClient()); 
    }
    
    /**
     * Read register from database.
     *
     * @return the register value as ByteString
     */
    public ByteString readRegisterValueBS() {
        ApbGetRegResp reg = getClient().readHelper(getName(), getBucket(), AntidoteType.LWWRegisterType).getObjects().getObjects(0).getReg();
        return reg.getValue(); 
    }
    
    /**
     * Read register from database.
     *
     * @param antidoteTransaction the antidote transaction
     * @return the register value as ByteString
     */
    public ByteString readRegisterValueBS(AntidoteTransaction antidoteTransaction){
    	ApbGetRegResp reg = antidoteTransaction.readHelper(getName(), getBucket(), AntidoteType.LWWRegisterType).getObjects(0).getReg();
        return reg.getValue(); 
    }
    
    /**
     * Read register from database.
     *
     * @return the register value as String
     */
    public String readRegisterValue() {
        ApbGetRegResp reg = getClient().readHelper(getName(), getBucket(), AntidoteType.LWWRegisterType).getObjects().getObjects(0).getReg();
        return reg.getValue().toStringUtf8(); 
    }
    
    /**
     * Read register from database.
     *
     * @param antidoteTransaction the antidote transaction
     * @return the register value as String
     */
    public String readRegisterValue(AntidoteTransaction antidoteTransaction){
    	ApbGetRegResp reg = antidoteTransaction.readHelper(getName(), getBucket(), AntidoteType.LWWRegisterType).getObjects(0).getReg();
        return reg.getValue().toStringUtf8(); 
    }
}
