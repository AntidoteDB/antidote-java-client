package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import com.basho.riak.protobuf.AntidotePB.ApbUpdateOperation;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;


/**
 * The Class AntidoteObject.
 */
public abstract class AntidoteCRDT {
    ObjectRef obj;
	/** The list of locally executed but not yet pushed operations. */
	private List<ApbUpdateOperation.Builder> updateList;

	/** The type. */
	private final CRDT_type type;
	
	/** The name. */
    private final String name;
    
    /** The bucket. */
    private final String bucket;
  
    /** The antidote client. */
	private final AntidoteClient antidoteClient;

	/**
     * Instantiates a new antidote object.
     *
     * @param name the name
     * @param bucket the bucket
     * @param antidoteClient the antidote client
     * @param type the type
     */
    public AntidoteCRDT(String name, String bucket, AntidoteClient antidoteClient, CRDT_type type) {
    	this.name = name;
        this.bucket = bucket;
        this.antidoteClient = antidoteClient;
        updateList = new ArrayList<>();
        this.type = type;
    }
    
    /**
     * Gets the type.
     *
     * @return the type
     */
    public CRDT_type getType(){
    	return type;
    }

	/**
	 * Gets the update list.
	 *
	 * @return the update list
	 */
	protected List<ApbUpdateOperation.Builder> getUpdateList(){
		return updateList;
	}
	
	/**
	 * Update add.
	 *
	 * @param update the update
	 */
	protected void updateAdd(ApbUpdateOperation.Builder update){
		updateList.add(update);
	}
	
	/**
	 * Clear update list.
	 */
	protected void clearUpdateList(){
		updateList.clear();
	}
	
	/**
	 * Push locally executed updates to database.
	 */
	public void push(){	
		for(ApbUpdateOperation.Builder update : getUpdateList()){
			obj.updateHelper(update, name, bucket, type);
		}
		clearUpdateList();
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
