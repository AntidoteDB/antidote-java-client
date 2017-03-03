package main.java.AntidoteClient;

/**
 * The Class AntidoteClient.
 */
public final class AntidoteClient {
    
    /**  Pool Manager. */
    private PoolManager poolManager;

    /**
     * Instantiates a new antidote client.
     *
     * @param poolManager the pool manager object
     */
    public AntidoteClient(PoolManager poolManager) {
        this.poolManager = poolManager;
    }
    
    /**
     * Send message to database.
     *
     * @param requestMessage the update message
     * @return the response
     */
    protected AntidoteMessage sendMessage(AntidoteRequest requestMessage) {
        return poolManager.sendMessage(requestMessage);
    }

    /**
     * Create antidote transaction.
     *
     * @return the antidote transaction
     */
    public AntidoteTransaction createTransaction(){
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
    public AntidoteTransaction createStaticTransaction(){
    	AntidoteStaticTransaction antidoteStaticTransaction = new AntidoteStaticTransaction(this);
        antidoteStaticTransaction.setTransactionStatus(AntidoteStaticTransaction.TransactionStatus.CREATED);
        antidoteStaticTransaction.startTransaction();
        return antidoteStaticTransaction;
    }
    
    public CounterRef counterRef(String name, String bucket){
    	return new CounterRef(name, bucket, this);
    }
    
    public AWMapRef awMapRef(String name, String bucket){
    	return new AWMapRef(name, bucket, this);
    }
    
    public GMapRef gMapRef(String name, String bucket){
    	return new GMapRef(name, bucket, this);
    }
    
    public ORSetRef orSetRef(String name, String bucket){
    	return new ORSetRef(name, bucket, this);
    }
    
    public RWSetRef rwSetRef(String name, String bucket){
    	return new RWSetRef(name, bucket, this);
    }
    
    public LWWRegisterRef lwwRegisterRef(String name, String bucket){
    	return new LWWRegisterRef(name, bucket, this);
    }
    
    public MVRegisterRef mvRegisterRef(String name, String bucket){
    	return new MVRegisterRef(name, bucket, this);
    }
    
    public IntegerRef integerRef(String name, String bucket){
    	return new IntegerRef(name, bucket, this);
    }
}

