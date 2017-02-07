package main.java.AntidoteClient;

/**
 * The Class LowLevelObject.
 */
public class LowLevelObject {
	/** The name. */
    private String name;
    
    /** The bucket. */
    private String bucket;
    
	/** The antidote client. */
	private AntidoteClient antidoteClient;
	
	/**
	 * Instantiates a new low level object.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param antidoteClient the antidote client
	 */
	public LowLevelObject(String name, String bucket, AntidoteClient antidoteClient){
		this.name = name;
        this.bucket = bucket;
        this.antidoteClient = antidoteClient;
	}
 
    /**
     * Gets the name.
     *
     * @return the name
     */
    public String getName(){
    	return name;
    }
    
    /**
     * Gets the bucket.
     *
     * @return the bucket
     */
    public String getBucket(){
    	return bucket;
    };
    
	/**
	 * Gets the client.
	 *
	 * @return the client
	 */
	public AntidoteClient getClient(){
		return antidoteClient;
	}
}
