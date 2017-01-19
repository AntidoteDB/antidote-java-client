package main.java.AntidoteClient;

import com.google.protobuf.ByteString;

public interface RegisterInterface {
	String getValue();
	
	ByteString getValueBS();
	
	void readDatabase();
	
	void rollBack();
	
	void synchronize();
	
	void setValue(String value);
	
	void setValue (ByteString value);
	
	void push();
}
