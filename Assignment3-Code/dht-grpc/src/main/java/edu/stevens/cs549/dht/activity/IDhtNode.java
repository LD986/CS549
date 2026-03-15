package edu.stevens.cs549.dht.activity;

import edu.stevens.cs549.dht.activity.DhtBase.Failed;
import edu.stevens.cs549.dht.activity.DhtBase.Invalid;
import edu.stevens.cs549.dht.events.IBindingEventListener;
import edu.stevens.cs549.dht.events.EventProducer;
import java.io.OutputStream;

/*
 * The part of the DHT business logic that is used in the CLI
 * (the business logic for a command line interface).
 */

public interface IDhtNode {
	
	/**
	 * Adding and deleting content at the local node.
	 */
	public String[] get(String k) throws Invalid;
	
	public void add(String k, String v) throws Invalid;
	
	public void delete(String k, String v) throws Invalid;
	
	/**
	 * Adding and deleting content in the network.
	 */
	public String[] getNet(String k) throws Failed;
	
	public void addNet(String k, String v) throws Failed;
	
	public void deleteNet(String k, String v) throws Failed;
	
	/**
	 * Insert this node into a DHT identified by host and port.
	 */
	public void join(String host, int port) throws Failed, Invalid;
	
	/**
	 * Display internal state at the CLI.
	 */
	public void display();
	
	public void routes();

	/**
	 * Server-side: Listening for binding events in the network (invoked by RPC)
	 */
	public void listenOn(int id, String key, EventProducer eventProducer);

	public void listenOff(int id, String key);


	/**
	 * Client-side: Listening for binding events in the network (invoked from CLI)
	 */
	public void startListening(String key, IBindingEventListener bindingEventListener) throws DhtBase.Failed;

	public void stopListening(String key) throws DhtBase.Failed;

	public void listeners(OutputStream out);
	
}
