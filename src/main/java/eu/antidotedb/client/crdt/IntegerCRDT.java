package eu.antidotedb.client.crdt;

import eu.antidotedb.client.AntidoteTransaction;

/**
 * The Interface InterfaceInteger.
 */
public interface IntegerCRDT {
	
	/**
	 * Gets the value.
	 *
	 * @return the value
	 */
	int getValue();
	
	/**
	 * Increment by 1.
	 */
	void increment(AntidoteTransaction antidoteTransaction);
	
	/**
	 * Increment.
	 */
	void increment(int inc, AntidoteTransaction antidoteTransaction);
	
	/**
	 * Sets the value.
	 */
	void setValue(int value, AntidoteTransaction antidoteTransaction); 
}
