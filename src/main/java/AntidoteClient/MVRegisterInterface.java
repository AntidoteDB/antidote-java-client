package main.java.AntidoteClient;

import java.util.List;

import com.google.protobuf.ByteString;

public interface MVRegisterInterface {
	List<String> getValueList();
	
	List<ByteString> getValueListBS();
	
	void readDatabase();
	
	void rollBack();
	
	void synchronize();
	
	void setValue(String value);
	
	void setValue (ByteString value);
	
	void push();
}
