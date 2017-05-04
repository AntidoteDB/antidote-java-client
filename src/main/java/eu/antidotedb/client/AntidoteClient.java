package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB.*;
import com.google.protobuf.*;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.toIntExact;

/**
 * The Class AntidoteClient.
 */
public final class AntidoteClient {

    private PoolManager poolManager;

    /**
     * Instantiates a new antidote client.
     *
     * @param poolManager the pool manager object
     */
    public AntidoteClient(PoolManager poolManager) {
        this.setPoolManager(poolManager);
    }

    /**
     * Send message to database.
     *
     * @param requestMessage the update message
     * @return the response
     */
    protected AntidoteMessage sendMessage(AntidoteRequest requestMessage) {
        return getPoolManager().sendMessage(requestMessage);
    }


    protected AntidoteMessage sendMessage(AntidoteRequest requestMessage, Connection connection) {
        return getPoolManager().sendMessage(requestMessage, connection);
    }

    /**
     * Create antidote transaction.
     *
     * @return the antidote transaction
     */
    public AntidoteTransaction createTransaction() {
        AntidoteTransaction antidoteTransaction = new AntidoteTransaction(this);
        antidoteTransaction.setTransactionStatus(AntidoteTransaction.TransactionStatus.CREATED);
        antidoteTransaction.startTransaction();
        return antidoteTransaction;
    }

    /**
     * Create antidote static transaction.
     *
     * @return the antidote static transaction
     */
    public AntidoteTransaction createStaticTransaction() {
        AntidoteStaticTransaction antidoteStaticTransaction = new AntidoteStaticTransaction(this);
        antidoteStaticTransaction.setTransactionStatus(AntidoteStaticTransaction.TransactionStatus.CREATED);
        antidoteStaticTransaction.startTransaction();
        return antidoteStaticTransaction;
    }

    public CounterRef counterRef(String name, String bucket) {
        return new CounterRef(name, bucket, this);
    }

    public AWMapRef awMapRef(String name, String bucket) {
        return new AWMapRef(name, bucket, this);
    }

    public GMapRef gMapRef(String name, String bucket) {
        return new GMapRef(name, bucket, this);
    }

    public ORSetRef orSetRef(String name, String bucket) {
        return new ORSetRef(name, bucket, this);
    }

    public RWSetRef rwSetRef(String name, String bucket) {
        return new RWSetRef(name, bucket, this);
    }

    public LWWRegisterRef lwwRegisterRef(String name, String bucket) {
        return new LWWRegisterRef(name, bucket, this);
    }

    public MVRegisterRef mvRegisterRef(String name, String bucket) {
        return new MVRegisterRef(name, bucket, this);
    }

    public IntegerRef integerRef(String name, String bucket) {
        return new IntegerRef(name, bucket, this);
    }

    public List<Object> readObjects(List<ObjectRef> objectRefs) {
        List<Object> objects = new ArrayList<Object>();
        for (ObjectRef objectRef : objectRefs) {
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
            AntidoteMessage responseMessage = this.sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStaticReadObjects, readMessageObject));
            ApbStaticReadObjectsResp readResponse = null;
            try {
                readResponse = ApbStaticReadObjectsResp.parseFrom(responseMessage.getMessage());
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
            CRDT_type crdt_type = object.getType();
            switch (crdt_type) {
                case COUNTER:
                    int counterValue = readResponse.getObjects().getObjects(0).getCounter().getValue();
                    objects.add(counterValue);
                    break;
                case INTEGER:
                    long integerValue = readResponse.getObjects().getObjects(0).getInt().getValue();
                    objects.add(toIntExact(integerValue));
                    break;
                case MVREG:
                    List<ByteString> mvRegisterValueList = readResponse.getObjects().getObjects(0).getMvreg().getValuesList();
                    objects.add(mvRegisterValueList);
                    break;
                case LWWREG:
                    ByteString lwwRegisterValue = readResponse.getObjects().getObjects(0).getReg().getValue();
                    objects.add(lwwRegisterValue);
                    break;
                case ORSET:
                    List<ByteString> orSetValueList = readResponse.getObjects().getObjects(0).getSet().getValueList();
                    objects.add(orSetValueList);
                    break;
                case RWSET:
                    List<ByteString> rwSetValueList = readResponse.getObjects().getObjects(0).getSet().getValueList();
                    objects.add(rwSetValueList);
                    break;
                case AWMAP:
                    List<ApbMapEntry> awMapEntryList = new ArrayList<ApbMapEntry>();
                    awMapEntryList = readResponse.getObjects().getObjects(0).getMap().getEntriesList();
                    objects.add(awMapEntryList);
                    break;
                case GMAP:
                    List<ApbMapEntry> gMapEntryList = new ArrayList<ApbMapEntry>();
                    gMapEntryList = readResponse.getObjects().getObjects(0).getMap().getEntriesList();
                    objects.add(gMapEntryList);
                    break;
            }
        }
        return objects;
    }

    public void readOuterObjects(List<AntidoteCRDT> objectRefs) {
        for (AntidoteCRDT objectRef : objectRefs) {

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
            AntidoteMessage responseMessage = this.sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStaticReadObjects, readMessageObject));

            ApbStaticReadObjectsResp readResponse = null;
            try {
                readResponse = ApbStaticReadObjectsResp.parseFrom(responseMessage.getMessage());
            } catch (InvalidProtocolBufferException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            CRDT_type crdt_type = objectRef.getType();
            switch (crdt_type) {
                case COUNTER:
                    int counterValue = readResponse.getObjects().getObjects(0).getCounter().getValue();
                    ((AntidoteOuterCounter) objectRef).readSetValue(counterValue);
                    break;
                case INTEGER:
                    long integerValue = readResponse.getObjects().getObjects(0).getInt().getValue();
                    ((AntidoteOuterInteger) objectRef).readSetValue(toIntExact(integerValue));
                    break;
                case MVREG:
                    List<ByteString> mvRegisterValueList = readResponse.getObjects().getObjects(0).getMvreg().getValuesList();
                    ((AntidoteOuterMVRegister) objectRef).readValueList(mvRegisterValueList);
                    break;
                case LWWREG:
                    ByteString lwwRegisterValue = readResponse.getObjects().getObjects(0).getReg().getValue();
                    ((AntidoteOuterLWWRegister) objectRef).readValueList(lwwRegisterValue);
                    break;
                case ORSET:
                    List<ByteString> orSetValueList = readResponse.getObjects().getObjects(0).getSet().getValueList();
                    ((AntidoteOuterORSet) objectRef).readValueList(orSetValueList);
                    break;
                case RWSET:
                    List<ByteString> rwSetValueList = readResponse.getObjects().getObjects(0).getSet().getValueList();
                    ((AntidoteOuterRWSet) objectRef).readValueList(rwSetValueList);
                    break;
                case AWMAP:
                    List<ApbMapEntry> awMapEntryList = new ArrayList<ApbMapEntry>();
                    awMapEntryList = readResponse.getObjects().getObjects(0).getMap().getEntriesList();
                    ((AntidoteOuterAWMap) objectRef).readSetValue(awMapEntryList);
                    break;
                case GMAP:
                    List<ApbMapEntry> gMapEntryList = new ArrayList<ApbMapEntry>();
                    gMapEntryList = readResponse.getObjects().getObjects(0).getMap().getEntriesList();
                    ((AntidoteOuterGMap) objectRef).readSetValue(gMapEntryList);
                    break;
            }

        }
    }

    /**
     * Pool Manager.
     */
    public PoolManager getPoolManager() {
        return poolManager;
    }

    public void setPoolManager(PoolManager poolManager) {
        this.poolManager = poolManager;
    }
}

