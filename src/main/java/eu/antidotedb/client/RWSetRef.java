package eu.antidotedb.client;

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
		super(name, bucket, antidoteClient, AntidoteType.RWSetType);
	}
    
    /**
     * Removes the value.
     *
     * @param element the element
     * @param antidoteTransaction the antidote transaction
     */
    public void removeBS(ByteString element, AntidoteTransaction antidoteTransaction){
    	super.removeBS(element, getType(), antidoteTransaction);
    }

    /**
     * Adds the value.
     *
     * @param element the element
     * @param antidoteTransaction the antidote transaction
     */
    public void addBS(ByteString element, AntidoteTransaction antidoteTransaction){
    	super.addBS(element, getType(), antidoteTransaction);
    }
    
    /**
     * Removes the value.
     *
     * @param element the element
     * @param antidoteTransaction the antidote transaction
     */
    public void remove(String element, AntidoteTransaction antidoteTransaction){
    	super.remove(element, getType(), antidoteTransaction);
    }
    
    /**
     * Adds the value.
     *
     * @param element the element
     * @param antidoteTransaction the antidote transaction
     */
    public void add(String element, AntidoteTransaction antidoteTransaction){
    	super.add(element, getType(), antidoteTransaction);
    }

    /**
     * Removes the value.
     *
     * @param elements the elements
     * @param antidoteTransaction the antidote transaction
     */
    public void removeBS(List<ByteString> elements, AntidoteTransaction antidoteTransaction){
    	super.removeBS(elements, getType(), antidoteTransaction);
    }

    /**
     * Adds the value.
     *
     * @param elements the elements
     * @param antidoteTransaction the antidote transaction
     */
    public void addBS(List<ByteString> elements, AntidoteTransaction antidoteTransaction){
    	super.addBS(elements, getType(), antidoteTransaction);
    }
    
    /**
     * Removes the value.
     *
     * @param elements the elements
     * @param antidoteTransaction the antidote transaction
     */
    public void remove(List<String> elements, AntidoteTransaction antidoteTransaction){
    	super.remove(elements, getType(), antidoteTransaction);
    }

    /**
     * Adds the value.
     *
     * @param elements the elements
     * @param antidoteTransaction the antidote transaction
     */
    public void add(List<String> elements, AntidoteTransaction antidoteTransaction){
    	super.add(elements, getType(), antidoteTransaction);
    }
    
    /**
     * Read RW-Set from database.
     *
     * @param antidoteTransaction the antidote transaction
     * @return the antidote RW set
     */
    public AntidoteOuterRWSet createAntidoteRWSet(AntidoteTransaction antidoteTransaction){
    	ApbGetSetResp set = antidoteTransaction.readHelper(getName(), getBucket(), getType()).getObjects(0).getSet();
        return new AntidoteOuterRWSet(getName(), getBucket(), set.getValueList(), getClient());

    }

    /**
     * Read RW-Set from database.
     *
     * @return the antidote RW set
     */
    public AntidoteOuterRWSet createAntidoteRWSet(){
        List<ByteString> rwSetValueList = (List<ByteString>) getObjectRefValue(this);
        AntidoteOuterRWSet antidoteSet = new AntidoteOuterRWSet(getName(), getBucket(), rwSetValueList, getClient());
        return antidoteSet;
    }
    
    /**
     * Read the value list from the data base.
     *
     * @param antidoteTransaction the antidote transaction
     * @return the value list as Strings
     */
    public List<String> readValueList(AntidoteTransaction antidoteTransaction){
    	ApbGetSetResp set = antidoteTransaction.readHelper(getName(), getBucket(), getType()).getObjects(0).getSet();
    	List<String> valueList = new ArrayList<String>();
        for (ByteString e : set.getValueList()){
        	valueList.add(e.toStringUtf8());
        }
        return valueList;
    }

    /**
     * Read the value list from the data base.
     *
     * @return the value list as Strings
     */
    public List<String> readValueList(){
        List<ByteString> rwSetValueList = (List<ByteString>) getObjectRefValue(this);
        List<String> valueList = new ArrayList<String>();
        for (ByteString e : rwSetValueList){
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
    	ApbGetSetResp set = antidoteTransaction.readHelper(getName(), getBucket(), getType()).getObjects(0).getSet();
        return set.getValueList();
    }

    /**
     * Read the value list from the data base.
     *
     * @return the value list as ByteStrings
     */
    public List<ByteString> readValueListBS(){
        List<ByteString> rwSetValueList = (List<ByteString>) getObjectRefValue(this);
        return rwSetValueList;
    }
}
