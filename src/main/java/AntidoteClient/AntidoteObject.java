package main.java.AntidoteClient;

public abstract class AntidoteObject {
    private String name;
    private String bucket;
    
    public AntidoteObject(String name, String bucket) {
        this.name = name;
        this.bucket = bucket;
    }
    
    public String getName(){
    	return name;
    }
    
    public String getBucket(){
    	return bucket;
    };
}
