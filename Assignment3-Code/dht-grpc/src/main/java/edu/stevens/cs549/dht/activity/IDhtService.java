package edu.stevens.cs549.dht.activity;

import edu.stevens.cs549.dht.activity.DhtBase.Failed;
import edu.stevens.cs549.dht.activity.DhtBase.Invalid;
import edu.stevens.cs549.dht.events.EventProducer;
import edu.stevens.cs549.dht.rpc.NodeBindings;
import edu.stevens.cs549.dht.rpc.NodeInfo;
import edu.stevens.cs549.dht.rpc.OptNodeBindings;
import edu.stevens.cs549.dht.rpc.OptNodeInfo;


/*
 * The part of the DHT business logic that is used in the Web service.
 */

public interface IDhtService {
	
	public NodeInfo getNodeInfo();
	
	public NodeInfo getSucc();
	
	public OptNodeInfo getPred();
	
	/*
	 * Called externally during the join protocol.
	 */
	public NodeInfo findSuccessor(int id) throws Failed;
	
	/*
	 * Exposing the finger table in the node API.
	 * Alternatively search for predecessor tail-recursively,
	 * but be sure to redirect HTTP to the next node.
	 */
	public NodeInfo closestPrecedingFinger (int id);
	
	/*
	 * Called by a node to notify another node that it believes it is the latter
	 * node's new predecessor.  If acknowledged by the successor, then a range of
	 * key-value pairs is transferred from the successor  (all bindings 
	 * at the successor up to and including the key of the new predecessor).
	 * The predecessor supplies its own bindings, to be backed up
	 * at the successor.
	 */
	public OptNodeBindings notify (NodeBindings predDb);
	
	/*
	 * The operations for actually accessing the underlying hash table for a node.
	 */
	public String[] get(String k) throws Invalid;
	
	public void add(String k, String v) throws Invalid;
	
	public void delete(String k, String v) throws Invalid;

	public void listenOn(int listenerId, String key, EventProducer eventProducer);

	public void listenOff(int listenerId, String key);

}
