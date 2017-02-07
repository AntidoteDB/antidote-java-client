package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import com.basho.riak.protobuf.AntidotePB.ApbGetMVRegResp;
import com.google.protobuf.ByteString;

/**
 * The Class LowLevelMVRegister.
 */
public class LowLevelMVRegister extends LowLevelRegister {
	
	/**
	 * Instantiates a new low level MV register.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param antidoteClient the antidote client
	 */
	public LowLevelMVRegister(String name, String bucket, AntidoteClient antidoteClient){
		super(name, bucket, antidoteClient);
	}
	
	/**
	 * Sets the value.
	 *
	 * @param value the value
	 */
	public void set(String value){
		super.set(value, AntidoteType.MVRegisterType);
	}
	
	/**
	 * Sets the value.
	 *
	 * @param value the new bs
	 */
	public void setBS(ByteString value){
		super.setBS(value, AntidoteType.MVRegisterType);
	}
	
	/**
	 * Sets the value.
	 *
	 * @param value the value
	 * @param antidoteTransaction the antidote transaction
	 */
	public void setBS(ByteString value, AntidoteTransaction antidoteTransaction){
		super.setBS(value, AntidoteType.MVRegisterType, antidoteTransaction);
	}
	
	/**
	 * Sets the value.
	 *
	 * @param value the value
	 * @param antidoteTransaction the antidote transaction
	 */
	public void set(String value, AntidoteTransaction antidoteTransaction){
		super.set(value, AntidoteType.MVRegisterType, antidoteTransaction);
	}
	
	 /**
     * Read register from database.
     *
     * @return the antidote register
     */
    public AntidoteOuterMVRegister createAntidoteMVRegister() {
        ApbGetMVRegResp reg = getClient().readHelper(getName(), getBucket(), AntidoteType.MVRegisterType).getObjects().getObjects(0).getMvreg();
        List<String> entriesList = new ArrayList<String>();
        for (ByteString e : reg.getValuesList()){
          	entriesList.add(e.toStringUtf8());
        }
        return new AntidoteOuterMVRegister(getName(), getBucket(), entriesList, getClient()); 
    }
    
    /**
     * Read register from database.
     *
     * @param antidoteTransaction the antidote transaction
     * @return the antidote register
     */
    public AntidoteOuterMVRegister createAntidoteMVRegister(AntidoteTransaction antidoteTransaction){
        ApbGetMVRegResp reg = antidoteTransaction.readHelper(getName(), getBucket(), AntidoteType.MVRegisterType).getObjects(0).getMvreg();         
    	List<String> entriesList = new ArrayList<String>();
        for (ByteString e : reg.getValuesList()){
          	entriesList.add(e.toStringUtf8());
        }
        return new AntidoteOuterMVRegister(getName(), getBucket(), entriesList, getClient()); 
    }
    
    /**
     * Read register from database.
     *
     * @return the register value as ByteString
     */
    public List<ByteString> readRegisterValuesBS() {
        ApbGetMVRegResp reg = getClient().readHelper(getName(), getBucket(), AntidoteType.MVRegisterType).getObjects().getObjects(0).getMvreg();
        return reg.getValuesList(); 
    }
    
    /**
     * Read register from database.
     *
     * @param antidoteTransaction the antidote transaction
     * @return the register value as ByteString
     */
    public List<ByteString> readRegisterValuesBS(AntidoteTransaction antidoteTransaction){
    	ApbGetMVRegResp reg = antidoteTransaction.readHelper(getName(), getBucket(), AntidoteType.MVRegisterType).getObjects(0).getMvreg();
        return reg.getValuesList(); 
    }
    
    /**
     * Read register from database.
     *
     * @return the register value as String
     */
    public List<String> readRegisterValues() {
        ApbGetMVRegResp reg = getClient().readHelper(getName(), getBucket(), AntidoteType.MVRegisterType).getObjects().getObjects(0).getMvreg();
        List<String> entriesList = new ArrayList<String>();
        for (ByteString e : reg.getValuesList()){
          	entriesList.add(e.toStringUtf8());
        }
        return entriesList; 
    }
    
    /**
     * Read register from database.
     *
     * @param antidoteTransaction the antidote transaction
     * @return the register value as String
     */
    public List<String> readRegisterValues(AntidoteTransaction antidoteTransaction){
    	ApbGetMVRegResp reg = antidoteTransaction.readHelper(getName(), getBucket(), AntidoteType.MVRegisterType).getObjects(0).getMvreg();
    	List<String> entriesList = new ArrayList<String>();
        for (ByteString e : reg.getValuesList()){
          	entriesList.add(e.toStringUtf8());
        }
        return entriesList; 
    }
}
