package main.java.AntidoteClient;

/**
 * The Interface CounterInterface.
 */
public interface CounterInterface {
	
	/**
	 * Gets the value.
	 *
	 * @return the value
	 */
	int getValue();
	
	/**
	 * Read database.
	 */
	void readDatabase();
	
	/**
	 * Roll back: delete information about local updates and read database.
	 */
	void rollBack();
	
	/**
	 * Synchronize: first push own changes, then read database.
	 */
	void synchronize();
	
	/**
	 * Increment.
	 */
	void increment();
	
	/**
	 * Increment.
	 *
	 * @param inc the inc
	 */
	void increment(int inc);
	
	/**
	 * Push locally executed updates to database. Uses a transaction.
	 */
	void push();
	
}
