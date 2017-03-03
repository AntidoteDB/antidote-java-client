package main.java.AntidoteClient;

import com.basho.riak.protobuf.AntidotePB.*;
import com.google.protobuf.*;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import static java.lang.Math.toIntExact;

/**
 * Created by george on 2/22/17.
 */
public final class AntidoteStaticTransaction extends AntidoteTransaction implements Closeable{

    private ApbStartTransaction.Builder startTransaction;

    /**
     * Instantiates a new antidote static transaction.
     *
     * @param antidoteClient the antidote client
     */
    public AntidoteStaticTransaction(AntidoteClient antidoteClient){
        super(antidoteClient);
        startTransaction = null;
    }

    /**
     * Start static transaction.
     *
     * @return the start transaction
     */
    protected void startTransaction(){
        if(getTransactionStatus() != TransactionStatus.CREATED){
            throw new AntidoteException("You need to create the transaction before starting it");
        }
        ApbTxnProperties.Builder transactionProperties = ApbTxnProperties.newBuilder();

        ApbStartTransaction.Builder writeTransaction = ApbStartTransaction.newBuilder();
        writeTransaction.setProperties(transactionProperties);

        setTransactionStatus(TransactionStatus.STARTED);
        startTransaction = writeTransaction;
    }

    /**
     * Commit static transaction.
     */
    public void commitTransaction(){
        if(getTransactionStatus() != TransactionStatus.STARTED){
            throw new AntidoteException("You need to start the transaction before committing it");
        }
        AntidoteMessage responseMessage = getClient().sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStaticUpdateObjects, createUpdateStaticObject()));
            try {
                ApbCommitResp commitResponse = ApbCommitResp.parseFrom(responseMessage.getMessage());
                System.out.println(commitResponse);
            } catch (Exception e ) {
                System.out.println(e);
            }

        setTransactionStatus(TransactionStatus.CLOSING);
    }

    /**
     * Abort static transaction.
     */
    public void abortTransaction(){
        if(getTransactionStatus() != TransactionStatus.STARTED){
            throw new AntidoteException("You need to start the transaction before aborting it");
        }
        clearTransactionUpdateList();
        setTransactionStatus(TransactionStatus.CLOSING);
    }

    /**
     * Creates the static update object.
     *
     * @return the static update object
     */
    protected ApbStaticUpdateObjects createUpdateStaticObject() {
        ApbStaticUpdateObjects.Builder updateMessage = ApbStaticUpdateObjects.newBuilder(); // Message which will be sent to antidote
        updateMessage.setTransaction(startTransaction);
        for(ApbUpdateOp.Builder updateInstruction : getTransactionUpdateList()) {
            updateMessage.addUpdates(updateInstruction);
        }
        clearTransactionUpdateList();
        ApbStaticUpdateObjects UpdateMessageObject = updateMessage.build();
        return UpdateMessageObject;
    }

    public List<AntidoteCRDT> readOuterObjects(List<AntidoteCRDT> objectRefs){
       List<AntidoteCRDT> objects = new ArrayList<AntidoteCRDT>();
        for(AntidoteCRDT objectRef : objectRefs) {

            ApbBoundObject.Builder object = ApbBoundObject.newBuilder(); // The object in the message
            object.setKey(ByteString.copyFromUtf8(objectRef.getName()));
            object.setType(objectRef.getType());
            object.setBucket(ByteString.copyFromUtf8(objectRef.getBucket()));

            ApbTxnProperties.Builder transactionProperties = ApbTxnProperties.newBuilder();

            ApbStartTransaction.Builder readTransaction = ApbStartTransaction.newBuilder();
            readTransaction.setProperties(transactionProperties);

            ApbStaticReadObjects.Builder readMessage = ApbStaticReadObjects.newBuilder();
            readMessage.setTransaction(readTransaction);
            readMessage.addObjects(object);

            ApbStaticReadObjects readMessageObject = readMessage.build();
            AntidoteMessage responseMessage = getClient().sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStaticReadObjects, readMessageObject));

            ApbStaticReadObjectsResp readResponse = null;
            try {
                readResponse = ApbStaticReadObjectsResp.parseFrom(responseMessage.getMessage());
            } catch (InvalidProtocolBufferException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            CRDT_type crdt_type = object.getType();
            switch (crdt_type){
                case COUNTER:
                    ApbGetCounterResp counter = readResponse.getObjects().getObjects(0).getCounter();
                    AntidoteOuterCounter antidoteCounter = new AntidoteOuterCounter(objectRef.getName(), objectRef.getBucket(), counter.getValue(), getClient());
                    objects.add(antidoteCounter);
                    break;
                case INTEGER:
                    ApbGetIntegerResp integer = readResponse.getObjects().getObjects(0).getInt();
                    AntidoteOuterInteger antidoteInteger = new AntidoteOuterInteger(objectRef.getName(), objectRef.getBucket(), toIntExact(integer.getValue()), getClient());
                    objects.add(antidoteInteger);
                    break;
            }

        }

        return objects;
    }

    public List<AntidoteCRDT> readObjects(List<ObjectRef> objectRefs){
        List<AntidoteCRDT> objects = new ArrayList<AntidoteCRDT>();
        for(ObjectRef objectRef : objectRefs) {

            ApbBoundObject.Builder object = ApbBoundObject.newBuilder(); // The object in the message
            object.setKey(ByteString.copyFromUtf8(objectRef.getName()));
                if(objectRef instanceof CounterRef){
                    object.setType(AntidoteType.CounterType);
                }
                else if(objectRef instanceof IntegerRef){
                    object.setType(AntidoteType.IntegerType);
                }
            object.setBucket(ByteString.copyFromUtf8(objectRef.getBucket()));

            ApbTxnProperties.Builder transactionProperties = ApbTxnProperties.newBuilder();

            ApbStartTransaction.Builder readTransaction = ApbStartTransaction.newBuilder();
            readTransaction.setProperties(transactionProperties);

            ApbStaticReadObjects.Builder readMessage = ApbStaticReadObjects.newBuilder();
            readMessage.setTransaction(readTransaction);
            readMessage.addObjects(object);

            ApbStaticReadObjects readMessageObject = readMessage.build();
            AntidoteMessage responseMessage = getClient().sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStaticReadObjects, readMessageObject));

            ApbStaticReadObjectsResp readResponse = null;
            try {
                readResponse = ApbStaticReadObjectsResp.parseFrom(responseMessage.getMessage());
            } catch (InvalidProtocolBufferException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            CRDT_type crdt_type = object.getType();
            switch (crdt_type){
                case COUNTER:
                    ApbGetCounterResp counter = readResponse.getObjects().getObjects(0).getCounter();
                    AntidoteOuterCounter antidoteCounter = new AntidoteOuterCounter(objectRef.getName(), objectRef.getBucket(), counter.getValue(), getClient());
                    objects.add(antidoteCounter);
                    break;
                case INTEGER:
                    ApbGetIntegerResp integer = readResponse.getObjects().getObjects(0).getInt();
                    AntidoteOuterInteger antidoteInteger = new AntidoteOuterInteger(objectRef.getName(), objectRef.getBucket(), toIntExact(integer.getValue()), getClient());
                    objects.add(antidoteInteger);
                    break;
            }

        }

        return objects;
    }
}
