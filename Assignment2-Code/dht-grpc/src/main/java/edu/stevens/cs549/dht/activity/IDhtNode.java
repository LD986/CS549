package edu.stevens.cs549.dht.activity;

import edu.stevens.cs549.dht.activity.DhtBase.Failed;
import edu.stevens.cs549.dht.activity.DhtBase.Invalid;

/*
 * The part of the DHT business logic that is used in the CLI
 * (the business logic for a command line interface).
 */

public interface IDhtNode {
	
	/*
	 * Adding and deleting content at the local node.
	 */
	public String[] get(String k) throws Invalid;
	
	public void add(String k, String v) throws Invalid;
	
	public void delete(String k, String v) throws Invalid;
	
	/*
	 * Adding and deleting content in the network.
	 */
	public String[] getNet(String k) throws Failed;
	
	public void addNet(String k, String v) throws Failed;
	
	public void deleteNet(String k, String v) throws Failed;
	
	/*
	 * Insert this node into a DHT identified by host and port.
	 */
	public void join(String host, int port) throws Failed, Invalid;
	
	/*
	 * Display internal state at the CLI.
	 */
	public void display();
	
	public void routes();
	
}
