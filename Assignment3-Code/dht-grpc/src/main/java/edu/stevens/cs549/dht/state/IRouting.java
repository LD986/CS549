package edu.stevens.cs549.dht.state;

import edu.stevens.cs549.dht.rpc.NodeInfo;
import edu.stevens.cs549.dht.rpc.OptNodeInfo;

/*
 * Interface for the RMI server that exposes the state of the routing tables.
 * ONLY to be invoked by the local business logic of the DHT node.
 */

public interface IRouting {

	public static final boolean USE_FINGER_TABLE = false;
	
	public static final int NFINGERS = 6;
	
	public static final int NKEYS = 64;
	
	public OptNodeInfo getPred();
	
	public void setPred(OptNodeInfo pred);
	
	public NodeInfo getSucc();
	
	public void setSucc(NodeInfo succ);
	
	public void setFinger (int i, NodeInfo info);
	
	public NodeInfo getFinger (int i);
	
	public NodeInfo closestPrecedingFinger (int id);
	
	public void routes();

}
