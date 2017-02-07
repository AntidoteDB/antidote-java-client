package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import com.basho.riak.protobuf.AntidotePB.ApbGetSetResp;
import com.google.protobuf.ByteString;

/**
 * The Class LowLevelORSet.
 */
public class LowLevelORSet extends LowLevelSet{
	
	/**
	 * Instantiates a new low level OR set.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param antidoteClient the antidote client
	 */
	public LowLevelORSet(String name, String bucket, AntidoteClient antidoteClient){
		super(name, bucket, antidoteClient);
	}
	
    /**
     * Removes the element.
     *
     * @param element the element
     */
    public void removeBS(ByteString element){
    	super.removeBS(element, AntidoteType.ORSetType);
    }

    /**
     * Adds the element.
     *
     * @param element the element
     */
    public void addBS(ByteString element){
    	super.addBS(element, AntidoteType.ORSetType);
    }
    
    /**
     * Removes the element.
     *
     * @param element the element
     */
    public void remove(String element){
    	super.remove(element, AntidoteType.ORSetType);
    }
    
    /**
     * Adds the element.
     *
     * @param element the element
     */
    public void add(String element){
    	super.add(element, AntidoteType.ORSetType);
    }

    /**
     * Removes the element.
     *
     * @param elements the elements
     */
    public void removeBS(List<ByteString> elements){
    	super.removeBS(elements, AntidoteType.ORSetType);
    }

    /**
     * Adds the element.
     *
     * @param elements the elements
     */
    public void addBS(List<ByteString> elements){
    	super.addBS(elements, AntidoteType.ORSetType);
    }
    
    /**
     * Removes the element.
     *
     * @param elements the elements
     */
    public void remove(List<String> elements){
    	super.remove(elements, AntidoteType.ORSetType);
    }

    /**
     * Adds the element.
     *
     * @param elements the elements
     */
    public void add(List<String> elements){
    	super.add(elements, AntidoteType.ORSetType);
    }
    
    /**
     * Removes the element.
     *
     * @param element the element
     * @param antidoteTransaction the antidote transaction
     */
    public void removeBS(ByteString element, AntidoteTransaction antidoteTransaction){
    	super.removeBS(element, AntidoteType.ORSetType, antidoteTransaction);
    }

    /**
     * Adds the element.
     *
     * @param element the element
     * @param antidoteTransaction the antidote transaction
     */
    public void addBS(ByteString element, AntidoteTransaction antidoteTransaction){
    	super.addBS(element, AntidoteType.ORSetType, antidoteTransaction);
    }
    
    /**
     * Removes the element.
     *
     * @param element the element
     * @param antidoteTransaction the antidote transaction
     */
    public void remove(String element, AntidoteTransaction antidoteTransaction){
    	super.remove(element, AntidoteType.ORSetType, antidoteTransaction);
    }
    
    /**
     * Adds the element.
     *
     * @param element the element
     * @param antidoteTransaction the antidote transaction
     */
    public void add(String element, AntidoteTransaction antidoteTransaction){
    	super.add(element, AntidoteType.ORSetType, antidoteTransaction);
    }

    /**
     * Removes the element.
     *
     * @param elements the elements
     * @param antidoteTransaction the antidote transaction
     */
    public void removeBS(List<ByteString> elements, AntidoteTransaction antidoteTransaction){
    	super.removeBS(elements, AntidoteType.ORSetType, antidoteTransaction);
    }

    /**
     * Adds the element.
     *
     * @param elements the elements
     * @param antidoteTransaction the antidote transaction
     */
    public void addBS(List<ByteString> elements, AntidoteTransaction antidoteTransaction){
    	super.addBS(elements, AntidoteType.ORSetType, antidoteTransaction);
    }
    
    /**
     * Removes the element.
     *
     * @param elements the elements
     * @param antidoteTransaction the antidote transaction
     */
    public void remove(List<String> elements, AntidoteTransaction antidoteTransaction){
    	super.remove(elements, AntidoteType.ORSetType, antidoteTransaction);
    }

    /**
     * Adds the element.
     *
     * @param elements the elements
     * @param antidoteTransaction the antidote transaction
     */
    public void add(List<String> elements, AntidoteTransaction antidoteTransaction){
    	super.add(elements, AntidoteType.ORSetType, antidoteTransaction);
    }
    
    /**
     * Read OR-Set from database.
     *
     * @return the antidote RW-Set
     */
    public AntidoteOuterORSet createAntidoteORSet() {
        ApbGetSetResp set = getClient().readHelper(getName(), getBucket(), AntidoteType.ORSetType).getObjects().getObjects(0).getSet();
        List<String> entriesList = new ArrayList<String>();
        for (ByteString e : set.getValueList()){
          	entriesList.add(e.toStringUtf8());
        }
        AntidoteOuterORSet antidoteSet = new AntidoteOuterORSet(getName(), getBucket(), entriesList, getClient());
        return antidoteSet;
    }
    
    /**
     * Read OR-Set from database.
     *
     * @param antidoteTransaction the antidote transaction
     * @return the antidote OR set
     */
    public AntidoteOuterORSet createAntidoteORSet(AntidoteTransaction antidoteTransaction){
    	ApbGetSetResp set = antidoteTransaction.readHelper(getName(), getBucket(), AntidoteType.ORSetType).getObjects(0).getSet();
        List<String> entriesList = new ArrayList<String>();
        for (ByteString e : set.getValueList()){
          	entriesList.add(e.toStringUtf8());
        }
        AntidoteOuterORSet antidoteSet = new AntidoteOuterORSet(getName(), getBucket(), entriesList, getClient());
        return antidoteSet;
    }
    
    /**
     * Read the value list from the data base.
     *
     * @return the value list as Strings
     */
    public List<String> readValueList() {
        ApbGetSetResp set = getClient().readHelper(getName(), getBucket(), AntidoteType.ORSetType).getObjects().getObjects(0).getSet();
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
     * @return the value list as Strings
     */
    public List<String> readValueList(AntidoteTransaction antidoteTransaction){
    	ApbGetSetResp set = antidoteTransaction.readHelper(getName(), getBucket(), AntidoteType.ORSetType).getObjects(0).getSet();
    	List<String> valueList = new ArrayList<String>();
        for (ByteString e : set.getValueList()){
        	valueList.add(e.toStringUtf8());
        }
        return valueList;
    }
    
    /**
     * Read the value list from the data base.
     *
     * @return the value list as ByteStrings
     */
    public List<ByteString> readValueListBS() {
        ApbGetSetResp set = getClient().readHelper(getName(), getBucket(), AntidoteType.ORSetType).getObjects().getObjects(0).getSet();
        return set.getValueList();
    }
    
    /**
     * Read the value list from the data base.
     *
     * @param antidoteTransaction the antidote transaction
     * @return the value list as ByteStrings
     */
    public List<ByteString> readValueListBS(AntidoteTransaction antidoteTransaction){
    	ApbGetSetResp set = antidoteTransaction.readHelper(getName(), getBucket(), AntidoteType.ORSetType).getObjects(0).getSet();
        return set.getValueList();
    }
}
