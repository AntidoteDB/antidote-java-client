package main.java.AntidoteClient;

public interface CounterInterface {
	int getValue();
	
	void readDatabase();
	
	void rollBack();
	
	void synchronize();
	
	void increment();
	
	void increment(int inc);
	
	void push();
	
}
