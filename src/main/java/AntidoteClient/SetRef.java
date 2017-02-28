package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import com.basho.riak.protobuf.AntidotePB.ApbSetUpdate;
import com.basho.riak.protobuf.AntidotePB.ApbUpdateOperation;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

/**
 * The Class LowLevelSet.
 */
public class SetRef extends ObjectRef {
	
	/**
	 * Instantiates a new low level set.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param antidoteClient the antidote client
	 */
	public SetRef(String name, String bucket, AntidoteClient antidoteClient){
		super(name, bucket, antidoteClient);
	}
	
	/**
	 * Prepare the remove operation builder.
	 *
	 * @param elements the elements
	 * @return the apb update operation. builder
	 */
	protected ApbUpdateOperation.Builder removeOpBuilder(List<ByteString> elements){
    	ApbSetUpdate.Builder setUpdateInstruction = ApbSetUpdate.newBuilder(); // The specific instruction in update instructions
        ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(2);
        setUpdateInstruction.setOptype(opType);
        setUpdateInstruction.addAllRems(elements);
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setSetop(setUpdateInstruction);
        return updateOperation;
    }
    
    /**
     * Prepare the add operation builder.
     *
     * @param elements the elements
     * @return the apb update operation. builder
     */
    protected ApbUpdateOperation.Builder addOpBuilder(List<ByteString> elements){
    	ApbSetUpdate.Builder setUpdateInstruction = ApbSetUpdate.newBuilder(); // The specific instruction in update instructions
    	ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(1);
        setUpdateInstruction.setOptype(opType);
        setUpdateInstruction.addAllAdds(elements);
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setSetop(setUpdateInstruction);
        return updateOperation;
    }

    /**
     * Removes the element.
     *
     * @param element the element
     * @param type the type
     */
    public void removeBS(ByteString element, CRDT_type type){
    	List<ByteString> elements = new ArrayList<>();
        elements.add(element);
        removeBS(elements, type);
    }

    /**
     * Adds the element.
     *
     * @param element the element
     * @param type the type
     */
    public void addBS(ByteString element, CRDT_type type){
        List<ByteString> elements = new ArrayList<>();
        elements.add(element);
        addBS(elements, type);
    }
    
    /**
     * Removes the element.
     *
     * @param element the element
     * @param type the type
     */
    public void remove(String element, CRDT_type type){
    	List<String> elements = new ArrayList<>();
        elements.add(element);
        remove(elements, type);
    }
    
    /**
     * Adds the element.
     *
     * @param element the element
     * @param type the type
     */
    public void add(String element, CRDT_type type){
        List<String> elements = new ArrayList<>();
        elements.add(element);
        add(elements, type);
    }

    /**
     * Removes the elements.
     *
     * @param elements the elements
     * @param type the type
     */
    public void removeBS(List<ByteString> elements, CRDT_type type){
        updateHelper(removeOpBuilder(elements), getName(), getBucket(), type);
    }

    /**
     * Adds the elements.
     *
     * @param elements the elements
     * @param type the type
     */
    public void addBS(List<ByteString> elements, CRDT_type type){
        updateHelper(addOpBuilder(elements), getName(), getBucket(), type);

    }
    
    /**
     * Removes the elements.
     *
     * @param elements the elements
     * @param type the type
     */
    public void remove(List<String> elements, CRDT_type type){
        List<ByteString> elementsByteString = new ArrayList<ByteString>();
        for (String e : elements){
        	elementsByteString.add(ByteString.copyFromUtf8(e));
        }
        updateHelper(removeOpBuilder(elementsByteString), getName(), getBucket(), type);
    }

    /**
     * Adds the elements.
     *
     * @param elements the elements
     * @param type the type
     */
    public void add(List<String> elements, CRDT_type type){
    	List<ByteString> elementsByteString = new ArrayList<ByteString>();
        for (String e : elements){
        	elementsByteString.add(ByteString.copyFromUtf8(e));
        }
        updateHelper(addOpBuilder(elementsByteString), getName(), getBucket(), type);
    }
    
    /**
     * Removes the element.
     *
     * @param element the element
     * @param type the type
     * @param antidoteTransaction the antidote transaction
     */
    public void removeBS(ByteString element, CRDT_type type, AntidoteTransaction antidoteTransaction){
    	List<ByteString> elements = new ArrayList<>();
        elements.add(element);
        removeBS(elements, type, antidoteTransaction);
    }

    /**
     * Adds the element.
     *
     * @param element the element
     * @param type the type
     * @param antidoteTransaction the antidote transaction
     */
    public void addBS(ByteString element, CRDT_type type, AntidoteTransaction antidoteTransaction){
        List<ByteString> elements = new ArrayList<>();
        elements.add(element);
        addBS(elements, type, antidoteTransaction);
    }
    
    /**
     * Removes the element.
     *
     * @param element the element
     * @param type the type
     * @param antidoteTransaction the antidote transaction
     */
    public void remove(String element, CRDT_type type, AntidoteTransaction antidoteTransaction){
    	List<String> elements = new ArrayList<>();
        elements.add(element);
        remove(elements, type, antidoteTransaction);
    }
    
    /**
     * Adds the element.
     *
     * @param element the element
     * @param type the type
     * @param antidoteTransaction the antidote transaction
     */
    public void add(String element, CRDT_type type, AntidoteTransaction antidoteTransaction){
        List<String> elements = new ArrayList<>();
        elements.add(element);
        add(elements, type, antidoteTransaction);
    }

    /**
     * Removes the elements.
     *
     * @param elements the elements
     * @param type the type
     * @param antidoteTransaction the antidote transaction
     */
    public void removeBS(List<ByteString> elements, CRDT_type type, AntidoteTransaction antidoteTransaction){
        antidoteTransaction.updateHelper(removeOpBuilder(elements), getName(), getBucket(), type);
    }

    /**
     * Adds the elements.
     *
     * @param elements the elements
     * @param type the type
     * @param antidoteTransaction the antidote transaction
     */
    public void addBS(List<ByteString> elements, CRDT_type type, AntidoteTransaction antidoteTransaction){
        antidoteTransaction.updateHelper(addOpBuilder(elements), getName(), getBucket(), type);
    }
    
    /**
     * Removes the elements.
     *
     * @param elements the elements
     * @param type the type
     * @param antidoteTransaction the antidote transaction
     */
    public void remove(List<String> elements, CRDT_type type, AntidoteTransaction antidoteTransaction){
        List<ByteString> elementsByteString = new ArrayList<ByteString>();
        for (String e : elements){
        	elementsByteString.add(ByteString.copyFromUtf8(e));
        }
        antidoteTransaction.updateHelper(removeOpBuilder(elementsByteString), getName(), getBucket(), type);

    }

    /**
     * Adds the elements.
     *
     * @param elements the elements
     * @param type the type
     * @param antidoteTransaction the antidote transaction
     */
    public void add(List<String> elements, CRDT_type type, AntidoteTransaction antidoteTransaction){
        List<ByteString> elementsByteString = new ArrayList<ByteString>();
        for (String e : elements){
        	elementsByteString.add(ByteString.copyFromUtf8(e));
        }
        antidoteTransaction.updateHelper(addOpBuilder(elementsByteString), getName(), getBucket(), type);
    }
}
