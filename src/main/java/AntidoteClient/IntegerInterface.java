package main.java.AntidoteClient;

public interface IntegerInterface {
	int getValue();
	
	void readDatabase();
	
	void rollBack();
	
	void synchronize();
	
	void increment();
	
	void increment(int inc);
	
	void push();
	
	void setValue(int value); 
}
