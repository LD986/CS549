package edu.stevens.cs549.dht.activity;

import edu.stevens.cs549.dht.activity.DhtBase.Error;
import edu.stevens.cs549.dht.activity.DhtBase.Failed;

/*
 * There are the DHT operations performed in the background.
 */
public interface IDhtBackground {
	
	/*
	 * Boolean result of stabilize tells us if successor node
	 * has accepted us as its predecessor.  
	 */
	public boolean stabilize() throws Error, Failed;
	
	/*
	 * If our predecessor has failed, set the pred pointer to nil.
	 */
	public void checkPredecessor() throws Error;
		
	/*
	 * Fix finger pointers (based on changes to successor pointers).
	 */
	public void fixFingers(int ntimes) throws Error, Failed;

}
