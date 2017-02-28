package main.java.AntidoteClient;

/**
 * Created by king on 12/6/2016.
 */
public class RiakPbMsgs {
    
    /** The Constant ApbStaticUpdateObjects. */
    //list of all msgs, better than enum because we don't need to call method to get value
    public static final int ApbStaticUpdateObjects = 122;
    
    /** The Constant ApbStaticReadObjects. */
    public static final int ApbStaticReadObjects = 123;
    
    /** The Constant ApbStartTransaction. */
    public static final int ApbStartTransaction = 119;
    
    /** The Constant ApbReadObjects. */
    public static final int ApbReadObjects = 116;
    
    /** The Constant ApbUpdateObjects. */
    public static final int ApbUpdateObjects = 118;
    
    /** The Constant ApbCommitTransaction. */
    public static final int ApbCommitTransaction = 121;

    /** The Constant ApbAbortTransaction. */
    public static final int ApbAbortTransaction = 120;
    //all other message codes can be added
}

