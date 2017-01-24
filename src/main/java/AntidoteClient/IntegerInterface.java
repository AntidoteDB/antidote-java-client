package main.java.AntidoteClient;

/**
 * The Interface IntegerInterface.
 */
public interface IntegerInterface {
	
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
	 * Increment by 1.
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
	
	/**
	 * Sets the value.
	 *
	 * @param value the new value
	 */
	void setValue(int value); 
}
