package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import com.basho.riak.protobuf.AntidotePB.ApbGetSetResp;
import com.google.protobuf.ByteString;

/**
 * The Class LowLevelORSet.
 */
public final class ORSetRef extends SetRef{


	
	/**
	 * Instantiates a new low level OR set.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param antidoteClient the antidote client
	 */
	public ORSetRef(String name, String bucket, AntidoteClient antidoteClient){
		super(name, bucket, antidoteClient, AntidoteType.ORSetType);

	}
	
    /**
     * Removes the element.
     *
     * @param element the element
     * @param antidoteTransaction the antidote transaction
     */
    public void removeBS(ByteString element, AntidoteTransaction antidoteTransaction){
    	super.removeBS(element, getType(), antidoteTransaction);
    }

    /**
     * Adds the element.
     *
     * @param element the element
     * @param antidoteTransaction the antidote transaction
     */
    public void addBS(ByteString element, AntidoteTransaction antidoteTransaction){
    	super.addBS(element, getType(), antidoteTransaction);
    }
    
    /**
     * Removes the element.
     *
     * @param element the element
     * @param antidoteTransaction the antidote transaction
     */
    public void remove(String element, AntidoteTransaction antidoteTransaction){
    	super.remove(element, getType(), antidoteTransaction);
    }
    
    /**
     * Adds the element.
     *
     * @param element the element
     * @param antidoteTransaction the antidote transaction
     */
    public void add(String element, AntidoteTransaction antidoteTransaction){
    	super.add(element, getType(), antidoteTransaction);
    }

    /**
     * Removes the element.
     *
     * @param elements the elements
     * @param antidoteTransaction the antidote transaction
     */
    public void removeBS(List<ByteString> elements, AntidoteTransaction antidoteTransaction){
    	super.removeBS(elements, getType(), antidoteTransaction);
    }

    /**
     * Adds the element.
     *
     * @param elements the elements
     * @param antidoteTransaction the antidote transaction
     */
    public void addBS(List<ByteString> elements, AntidoteTransaction antidoteTransaction){
    	super.addBS(elements, getType(), antidoteTransaction);
    }
    
    /**
     * Removes the element.
     *
     * @param elements the elements
     * @param antidoteTransaction the antidote transaction
     */
    public void remove(List<String> elements, AntidoteTransaction antidoteTransaction){
    	super.remove(elements, getType(), antidoteTransaction);
    }

    /**
     * Adds the element.
     *
     * @param elements the elements
     * @param antidoteTransaction the antidote transaction
     */
    public void add(List<String> elements, AntidoteTransaction antidoteTransaction){
    	super.add(elements, getType(), antidoteTransaction);
    }
    
    /**
     * Read OR-Set from database.
     *
     * @param antidoteTransaction the antidote transaction
     * @return the antidote OR set
     */
    public AntidoteOuterORSet createAntidoteORSet(AntidoteTransaction antidoteTransaction){
    	ApbGetSetResp set = antidoteTransaction.readHelper(getName(), getBucket(), getType()).getObjects(0).getSet();
        return new AntidoteOuterORSet(getName(), getBucket(), set.getValueList(), getClient());

    }

    /**
     * Read OR-Set from database.
     *
     * @return the antidote OR set
     */
    public AntidoteOuterORSet createAntidoteORSet(){
    List<ByteString> orSetValueList = (List<ByteString>) getObjectRefValue(this);
    AntidoteOuterORSet antidoteSet = new AntidoteOuterORSet(getName(), getBucket(), orSetValueList, getClient());
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
        List<ByteString> orSetValueList = (List<ByteString>) getObjectRefValue(this);
        List<String> valueList = new ArrayList<String>();
        for (ByteString e : orSetValueList){
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
        List<ByteString> orSetValueList = (List<ByteString>) getObjectRefValue(this);
        return orSetValueList;
    }
}
