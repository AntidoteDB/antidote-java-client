package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import com.basho.riak.protobuf.AntidotePB.ApbGetSetResp;
import com.google.protobuf.ByteString;

/**
 * The Class LowLevelRWSet.
 */
public final class RWSetRef extends SetRef{
	
	/**
	 * Instantiates a new low level RW set.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param antidoteClient the antidote client
	 */
	public RWSetRef(String name, String bucket, AntidoteClient antidoteClient){
		super(name, bucket, antidoteClient);
	}
    
    /**
     * Removes the value.
     *
     * @param element the element
     * @param antidoteTransaction the antidote transaction
     */
    public void removeBS(ByteString element, AntidoteTransaction antidoteTransaction){
    	super.removeBS(element, AntidoteType.RWSetType, antidoteTransaction);
    }

    /**
     * Adds the value.
     *
     * @param element the element
     * @param antidoteTransaction the antidote transaction
     */
    public void addBS(ByteString element, AntidoteTransaction antidoteTransaction){
    	super.addBS(element, AntidoteType.RWSetType, antidoteTransaction);
    }
    
    /**
     * Removes the value.
     *
     * @param element the element
     * @param antidoteTransaction the antidote transaction
     */
    public void remove(String element, AntidoteTransaction antidoteTransaction){
    	super.remove(element, AntidoteType.RWSetType, antidoteTransaction);
    }
    
    /**
     * Adds the value.
     *
     * @param element the element
     * @param antidoteTransaction the antidote transaction
     */
    public void add(String element, AntidoteTransaction antidoteTransaction){
    	super.add(element, AntidoteType.RWSetType, antidoteTransaction);
    }

    /**
     * Removes the value.
     *
     * @param elements the elements
     * @param antidoteTransaction the antidote transaction
     */
    public void removeBS(List<ByteString> elements, AntidoteTransaction antidoteTransaction){
    	super.removeBS(elements, AntidoteType.RWSetType, antidoteTransaction);
    }

    /**
     * Adds the value.
     *
     * @param elements the elements
     * @param antidoteTransaction the antidote transaction
     */
    public void addBS(List<ByteString> elements, AntidoteTransaction antidoteTransaction){
    	super.addBS(elements, AntidoteType.RWSetType, antidoteTransaction);
    }
    
    /**
     * Removes the value.
     *
     * @param elements the elements
     * @param antidoteTransaction the antidote transaction
     */
    public void remove(List<String> elements, AntidoteTransaction antidoteTransaction){
    	super.remove(elements, AntidoteType.RWSetType, antidoteTransaction);
    }

    /**
     * Adds the value.
     *
     * @param elements the elements
     * @param antidoteTransaction the antidote transaction
     */
    public void add(List<String> elements, AntidoteTransaction antidoteTransaction){
    	super.add(elements, AntidoteType.RWSetType, antidoteTransaction);
    }
    
    /**
     * Read RW-Set from database.
     *
     * @param antidoteTransaction the antidote transaction
     * @return the antidote RW set
     */
    public AntidoteOuterRWSet createAntidoteRWSet(AntidoteTransaction antidoteTransaction){
    	ApbGetSetResp set = readHelper(getName(), getBucket(), AntidoteType.RWSetType, antidoteTransaction).getObjects(0).getSet();
        AntidoteOuterRWSet antidoteSet = new AntidoteOuterRWSet(getName(), getBucket(), set.getValueList(), getClient());
        return antidoteSet;
    }
    
    /**
     * Read the value list from the data base.
     *
     * @param antidoteTransaction the antidote transaction
     * @return the value list as Strings
     */
    public List<String> readValueList(AntidoteTransaction antidoteTransaction){
    	ApbGetSetResp set = readHelper(getName(), getBucket(), AntidoteType.RWSetType, antidoteTransaction).getObjects(0).getSet();
    	List<String> valueList = new ArrayList<String>();
        for (ByteString e : set.getValueList()){
        	valueList.add(e.toStringUtf8());
        }
        return valueList;
    }
    
    /**
     * Read the value list from the data base.
     *
     * @param antidoteTransaction the antidote transaction
     * @return the value list as ByteStrings
     */
    public List<ByteString> readValueListBS(AntidoteTransaction antidoteTransaction){
    	ApbGetSetResp set = readHelper(getName(), getBucket(), AntidoteType.RWSetType, antidoteTransaction).getObjects(0).getSet();
        return set.getValueList();
    }
}
