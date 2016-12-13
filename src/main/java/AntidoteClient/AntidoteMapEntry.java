package main.java.AntidoteClient;

import java.util.List;

import com.basho.riak.protobuf.AntidotePB.ApbMapKey;

public class AntidoteMapEntry {
	private AntidoteClient antidoteClient;
	private List<ApbMapKey> path;
	private String name;
	private String bucket;
	
	public AntidoteMapEntry(AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path){
		this.name = name;
		this.antidoteClient = antidoteClient;
		this.bucket = bucket;
		this.path = path;
	}
	
	public String getName(){
		return name;
	}
	
	public String getBucket(){
		return bucket;
	}
	
	public List<ApbMapKey> getPath(){
		return path;
	}
	
	public AntidoteClient getClient(){
		return antidoteClient;
	}
}
