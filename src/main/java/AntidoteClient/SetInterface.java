package main.java.AntidoteClient;

import java.util.List;

import com.google.protobuf.ByteString;

public interface SetInterface {
	List<String> getValueList();
	
	List<ByteString> getValueListBS();
	
	void readDatabase();
	
	void rollBack();
	
	void synchronize();
	
	void addElement(String element);
	
	void addElement(List<String> elementList);
	
	void removeElement(String element);
	
	void removeElement(List<String> elementList);
	
	void addElementBS(ByteString element);
	
	void addElementBS(List<ByteString> elementList);
	
	void removeElementBS(ByteString element);
	
	void removeElementBS(List<ByteString> elementList);
	
	void push();
}
