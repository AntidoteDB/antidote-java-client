package interfaces;

import main.java.AntidoteClient.AntidoteTransaction;

/**
 * The Interface InterfaceCounter.
 */
public interface CounterCRDT{
	
	/**
	 * Gets the value.
	 *
	 * @return the value
	 */
	int getValue();
	
	/**
	 * Increment.
	 */
	void increment(AntidoteTransaction antidoteTransaction);
	
	/**
	 * Increment.
	 *
	 * @param inc the inc
	 */
	void increment(int inc, AntidoteTransaction antidoteTransaction);
}
