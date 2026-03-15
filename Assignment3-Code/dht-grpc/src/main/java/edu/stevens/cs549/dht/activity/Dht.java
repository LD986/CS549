package edu.stevens.cs549.dht.activity;

import edu.stevens.cs549.dht.events.IBindingEventListener;
import edu.stevens.cs549.dht.events.IEventListener;
import edu.stevens.cs549.dht.events.EventProducer;
import edu.stevens.cs549.dht.main.Log;
import edu.stevens.cs549.dht.main.WebClient;
import edu.stevens.cs549.dht.rpc.Bindings;
import edu.stevens.cs549.dht.rpc.NodeBindings;
import edu.stevens.cs549.dht.rpc.NodeInfo;
import edu.stevens.cs549.dht.rpc.OptNodeBindings;
import edu.stevens.cs549.dht.rpc.OptNodeInfo;
import edu.stevens.cs549.dht.rpc.Subscription;
import edu.stevens.cs549.dht.state.IRouting;
import edu.stevens.cs549.dht.state.IState;
import edu.stevens.cs549.dht.state.State;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Dht extends DhtBase implements IDhtService, IDhtNode, IDhtBackground {

	/*
	 * DHT logic.
	 * 
	 * This logic may be invoked from a RESTful Web service or from the command
	 * line, as reflected by the interfaces.
	 */

	/*
	 * Client stub for Web service calls.
	 */
	protected WebClient client;

	public WebClient getClient() {
		return client;
	}

	/*
	 * Logging operations.
	 */

	protected static final String TAG = Dht.class.getCanonicalName();
	
	private final Logger logger = Logger.getLogger(TAG);

	private void info(String s) {
		logger.info(s);
	}

	private void warning(String s) {
		logger.warning(s);
	}

	private void severe(String s) {
		logger.severe(s);
	}

	/*
	 * The URL for this node in the DHT. Although the info is stored in state,
	 * we should be able to cache it locally since it does not change.
	 */
	protected NodeInfo info;

	/*
	 * Remote clients may call this when joining the network and they know only
	 * our URI and need the node identifier (key) as well.
	 */
	// WebMethod
	public NodeInfo getNodeInfo() {
		assert this.info != null;
		return this.info;
	}

	/*
	 * Key-value pairs stored in this node.
	 */
	private final IState state;

	/*
	 * Finger tables, and predecessor and successor pointers.
	 */
	private final IRouting routing;

	/*
	 * This constructor is called when a DHT object is created for the CLI.
	 */

	/*
	 * This constructor is called when a DHT object is created in a Web service.
	 */
	private Dht() {
		this.state = State.getState();
		this.client = WebClient.getInstance(state);
		this.routing = (IRouting) state;
		this.info = state.getNodeInfo();
	}

	public static Dht getDht() {
		return new Dht();
	}

	/*
	 * Get the successor of a node. Need to perform a Web service call to that
	 * node, and it then retrieves its routing state from its local RMI object.
	 * 
	 * Make a special case for when this is the local node, i.e.,
	 * info.addr.equals(localInfo.addr), otherwise get an infinite loop.
	 */
	private NodeInfo getSucc(NodeInfo info) throws Failed {
		NodeInfo localInfo = this.getNodeInfo();
		if (isEqual(localInfo, info)) {
			return getSucc();
		} else {
			// TODO: Do the Web service call
			return null;
		}
	}

	/*
	 * This version gets the local successor from routing tables.
	 */
	// WebMethod
	public NodeInfo getSucc() {
		return routing.getSucc();
	}

	/*
	 * Set the local successor pointer in the routing tables.
	 */
	public void setSucc(NodeInfo succ) {
		routing.setSucc(succ);
	}

	/*
	 * Get the predecessor of a node. Need to perform a Web service call to that
	 * node, and it then retrieves its routing state from its local routing
	 * tables.
	 * 
	 * Make a special case for when this is the local node, i.e.,
	 * info.addr.equals(localInfo.addr), otherwise get an infinite loop.
	 */
	protected OptNodeInfo getPred(NodeInfo info) throws Failed {
		NodeInfo localInfo = this.getNodeInfo();
		if (isEqual(localInfo, info)) {
			return getPred();
		} else {
			/*
			 * TODO: Do the Web service call
			 */
			return null;
		}
	}

	/*
	 * This version gets the local predecessor from routing tabes.
	 */
	// WebMethod
	public OptNodeInfo getPred() {
		return routing.getPred();
	}

	/*
	 * Set the local predecessor pointer in the routing tables.
	 */
	protected void setPred(OptNodeInfo pred) {
		routing.setPred(pred);
	}

	/*
	 * Perform a Web service call to get the closest preceding finger in the
	 * finger table of the argument node.
	 */
	protected NodeInfo closestPrecedingFinger(NodeInfo info, int id) throws Failed {
		NodeInfo localInfo = this.getNodeInfo();
		if (isEqual(localInfo, info)) {
			return closestPrecedingFinger(id);
		} else {
			if (IRouting.USE_FINGER_TABLE) {
				/*
				 * TODO: Do the Web service call to the remote node.
				 */
				return null;
			} else {
				/*
				 * Without finger tables, just use the successor pointer.
				 */
				return getSucc(info);
			}
		}
	}

	/*
	 * For the local version, get from the routing table.
	 */
	// WebMethod
	public NodeInfo closestPrecedingFinger(int id) {
		return routing.closestPrecedingFinger(id);
	}

	/*
	 * Set a finger pointer.
	 */
	protected void setFinger(int ix, NodeInfo node) {
		routing.setFinger(ix, node);
	}

	/*
	 * Find the node that will hold bindings for a key k. Search the circular
	 * list to find the node. Stop when the node's successor stores values
	 * greater than k.
	 */
	protected NodeInfo findPredecessor(int id) throws Failed {
		NodeInfo info = getNodeInfo();
		NodeInfo succ = getSucc(info);
		if (info.getId() != succ.getId()) {
			while (!inInterval(id, info.getId(), succ.getId())) {
				info = closestPrecedingFinger(info, id);
				succ = getSucc(info);
				/*
				 * Break out of infinite loop (e.g. our successor may be
				 * a single-node network that still points to itself).
				 */
				if (isEqual(getNodeInfo(), info)) {
					break;
				}
			}
		}
		return info;
	}

	/*
	 * Find the successor of k, starting at the current node. Called internally
	 * and (server-side) in join protocol.
	 */
	// WebMethod
	public NodeInfo findSuccessor(int id) throws Failed {
		NodeInfo predInfo = findPredecessor(id);
		return getSucc(predInfo);
	}

	/*
	 * Stabilization logic.
	 * 
	 * Called by background thread & in join protocol by joining node (as new
	 * predecessor).
	 */
	public boolean stabilize() throws Failed {
		Log.background(TAG, "Starting stabilize()");
		NodeInfo info = getNodeInfo();
		NodeInfo succ = getSucc();
		if (isEqual(info, succ)) {
			Log.background(TAG, "Ending stabilize() 1");
			return true;
		}

		// Possible synchronization point (see join() below).
		OptNodeInfo predOfSucc = getPred(succ);
		if (!predOfSucc.hasNodeInfo()) {
			/*
			 * Successor's predecessor is not set, so we will become pred.
			 * Notify succ that we believe we are its predecessor. Provide our
			 * bindings, that will be backed up by our successor. Expect
			 * transfer of bindings from succ as ack.
			 * 
			 * Note: We pass in our bindings to the successor for backing up.
			 * We don't use this now, but might in a future assignment.
			 * 
			 * Do the Web service call.
			 */
			Log.debug(TAG, "Joining succ with null pred.");
			OptNodeBindings db = client.notify(succ, state.extractBindings());
			Log.background(TAG, "Ending stabilize() 2");
			return notifyContinue(db);
		} else if (inInterval(predOfSucc.getNodeInfo().getId(), info.getId(), succ.getId(), false)) {
			setSucc(predOfSucc.getNodeInfo());
			/*
			 * Successor's predecessor should be our predecessor. Notify pred of
			 * succ that we believe we are its predecessor. Expect transfer of
			 * bindings from succ as ack.
			 * 
			 * Do the Web service call.
			 */
			Log.debug(TAG, "Joining succ as new, closer pred.");
			/*
			 * Note: We notify predOfSucc in this case, since we have
			 * set that as our successor.
			 */
			OptNodeBindings db = client.notify(predOfSucc.getNodeInfo(), state.extractBindings());
			Log.background(TAG, "Ending stabilize() 3");
			// The bindings we got back will be merged with our current bindings.
			return notifyContinue(db);
		} else if (inInterval(info.getId(), predOfSucc.getNodeInfo().getId(), succ.getId(), false)) {
			/*
			 * Has some node inserted itself between us and the successor? This
			 * could happen due to a race condition between setting our
			 * successor pointer and notifying the successor.
			 */
			Log.debug(TAG, "Notifying succ that we are its pred.");
			OptNodeBindings db = client.notify(succ, state.extractBindings());
			Log.background(TAG, "Ending stabilize() 4");
			return notifyContinue(db);
		} else {
			/*
			 * We come here if we are already the predecessor, so no change.
			 */
			Log.background(TAG, "Ending stabilize() 5");
			return false;
		}
	}

	// WebMethod
	public OptNodeBindings notify(NodeBindings predDb) {
		/*
		 * Node cand ("candidate") believes it is our predecessor.
		 */
		NodeInfo cand = predDb.getInfo();
		OptNodeInfo pred = getPred();
		NodeInfo info = getNodeInfo();
		if (!pred .hasNodeInfo() || inInterval(cand.getId(), pred.getNodeInfo().getId(), info.getId(), false)) {
			Log.debug(TAG, String.format("notify: Node %d is being inserted between us and previous predecessor", cand.getId()));
			OptNodeInfo candPred = OptNodeInfo.newBuilder().setNodeInfo(cand).build();
			setPred(candPred);
			/*
			 * If this is currently a single-node network, close the loop by
			 * setting our successor to our pred. Thereafter the network should
			 * update automatically as new predecessors are detected by old
			 * predecessors.
			 */
			if (isEqual(getSucc(), info)) {
				setSucc(cand);
				// We must also set pred of succ (i.e. cand) to point back
				// to us. This will be done by stabilize().
			}
			/*
			 * Transfer our bindings up to cand.id to our new pred (the
			 * transferred bindings will be in the response). We will back up
			 * their bindings (this was sent as an argument in notify()).
			 */
			NodeBindings db;
			synchronized (state) {
				Log.debug(TAG, "notify: Transferring bindings back to new predecessor with id "+cand.getId());
				db = transferBindings(cand.getId());
				/*
				 * TODO Notify any listeners that the bindings have moved.
				 */
				Log.debug(TAG, "notify: Informing any nodes with listeners for transferred bindings");

			}

			// Back up predecessor bindings. Might be used in future assignment.
			state.backupBindings(predDb);
			Log.debug(TAG, String.format("Transferring %d bindings back to node id=%d", db.getBindingsList().size(), cand.getId()));
			return OptNodeBindings.newBuilder().setNodeBindings(db).build();
		} else {
			/*
			 * Null indicates that we did not accept new pred. This may be
			 * because cand==pred.
			 */
			return OptNodeBindings.getDefaultInstance();
		}
	}

	/*
	 * Process the result of a notify to a potential successor node.
	 */
	protected boolean notifyContinue(OptNodeBindings db) {
		if (!db.hasNodeBindings()) {
			/*
			 * We are out.
			 */
			return false;
		} else {
			/*
			 * Set pred pointer for the case where our successor is also our
			 * predecessor?
			 */
			NodeBindings nodeBindings = db.getNodeBindings();
			if (isEqual(getNodeInfo(), nodeBindings.getSucc())) {
				OptNodeInfo pred = OptNodeInfo.newBuilder().setNodeInfo(nodeBindings.getInfo()).build();
				setPred(pred);
			}
			/*
			 * db is bindings we take from the successor.
			 */
			Log.debug(TAG, String.format("Installing %d bindings from successor.", db.getNodeBindings().getBindingsList().size()));
			state.installBindings(nodeBindings);
			return true;
		}
	}

	/*
	 * Transfer our bindings up to predId to a new predecessor.
	 */
	protected NodeBindings transferBindings(int predId) {
		NodeBindings db = state.extractBindings(predId);
		state.dropBindings(predId);
		return db;
	}

	private int next = 0;

	/*
	 * Periodic refresh of finger table based on changed successor.
	 */
	protected void fixFinger() {
		int localNext;
		synchronized(state) {
			next = (next + 1) % IRouting.NFINGERS;
			localNext = next;
		}
		/*
		 * Compute offset = 2^next
		 */
		int offset = 1;
		for (int i = 0; i < localNext; i++)
			offset = offset * 2;
		/*
		 * finger[next] = findSuccessor (n + 2^next)
		 */
		int nextId = (getNodeInfo().getId() + offset) % IRouting.NKEYS;
		try {
			setFinger(localNext, findSuccessor(nextId));
		} catch (Failed e) {
			warning("FixFinger: findSuccessor(" + nextId + ") failed.");
		}
	}

	/*
	 * Speed up the finger table refresh.
	 */
	// Called by background thread.
	public void fixFingers(int ntimes) throws java.lang.Error {
		for (int i = 0; i < ntimes; i++) {
			fixFinger();
		}
	}

	/*
	 * Check if the predecessor has failed.
	 */
	// Called by background thread.
	public void checkPredecessor() {
		/*
		 * Ping the predecessor node by asking for its successor.
		 */
		OptNodeInfo pred = getPred();
		if (pred.hasNodeInfo()) {
			try {
				getSucc(pred.getNodeInfo());
			} catch (Failed e) {
				info("CheckPredecessor: Predecessor has failed (id=" + pred.getNodeInfo().getId() + ")");
				setPred(OptNodeInfo.getDefaultInstance());
			}
		}
	}

	/*
	 * Get the values under a key at the specified node. If the node is the
	 * current one, go to the local state.
	 */
	protected String[] get(NodeInfo n, String k) throws Failed {
		if (isEqual(n, getNodeInfo())) {
			try {
				return this.get(k);
			} catch (Invalid e) {
				severe("Get: invalid internal inputs: " + e);
				throw new IllegalArgumentException(e);
			}
		} else {
			/*
			 * Retrieve the bindings at the specified node.
			 * 
			 * TODO: Do the Web service call.
			 */
			return null;
		}
	}

	/*
	 * Retrieve values under the key at the current node.
	 */
	// WebMethod
	public String[] get(String k) throws Invalid {
		return state.get(k);
	}

	/*
	 * Add a value under a key.
	 */
	public void add(NodeInfo n, String k, String v) throws Failed {
		if (isEqual(n, getNodeInfo())) {
			try {
				add(k, v);
			} catch (Invalid e) {
				severe("Add: invalid internal inputs: " + e);
				throw new IllegalArgumentException(e);
			}
		} else {
			/*
			 * TODO: Do the Web service call.
			 */
		}
	}

	/*
	 * Store a value under a key at the local node.
	 */
	// WebMethod
	public void add(String k, String v) throws Invalid {
		/*
		 * Validate that this binding can be stored here.
		 */
		int kid = NodeKey(k);
		NodeInfo info = getNodeInfo();
		OptNodeInfo pred = getPred();

		if (pred.hasNodeInfo() && inInterval(kid, pred.getNodeInfo().getId(), info.getId(), true)) {
			/*
			 * This node covers the interval in which k should be stored.
			 */
			state.add(k, v);
			/*
			 * TODO Notify any listeners
			 */

		} else if (!pred.hasNodeInfo() && isEqual(info, getSucc())) {
			/*
			 * Single-node network.
			 */
			state.add(k, v);
			/*
			 * TODO Notify any listeners
			 */

		} else if (info.getId() == kid) {
			/*
			 * ???
			 */
			state.add(k, v);
			/*
			 * TODO Notify any listeners
			 */

		} else if (!pred.hasNodeInfo() && !isEqual(info, getSucc())) {
			severe("Add: predecessor is null but not a single-node network.");
		} else {
			throw new Invalid("Invalid key: " + k + " (id=" + kid + ")");
		}
	}

	/*
	 * Delete value under a key.
	 */
	public void delete(NodeInfo n, String k, String v) throws Failed {
		if (isEqual(n, getNodeInfo())) {
			try {
				delete(k, v);
			} catch (Invalid e) {
				severe("Delete: invalid internal inputs: " + e);
				throw new IllegalArgumentException(e);
			}
		} else {
			/*
			 * TODO: Do the Web service call.
			 */

		}
	}

	/*
	 * Delete value under a key at the local node.
	 */
	// WebMethod
	public void delete(String k, String v) throws Invalid {
		state.delete(k, v);
	}

	/*
	 * These operations perform the CRUD logic at the network level.
	 */

	/*
	 * Store a value under a key in the network.
	 */
	public void addNet(String skey, String v) throws Failed {
		int id = NodeKey(skey);
		//if the key's id is equal to the nodes id then add it locally
		if (info.getId() == id)
			add(info, skey, v);
		else {
			NodeInfo succ = this.findSuccessor(id);
			add(succ, skey, v);
		}
	}

	/*
	 * Get the values under a key in the network.
	 */
	public String[] getNet(String skey) throws Failed {
		int id = NodeKey(skey);
		// if the key's id is equal to the nodes id then get it locally
		if (info.getId() == id)
			return get(info, skey);
		else {
			NodeInfo succ = this.findSuccessor(id);
			return get(succ, skey);
		}
	}

	/*
	 * Delete a value under a key in the network.
	 */
	public void deleteNet(String skey, String v) throws Failed {
		int id = NodeKey(skey);
		//if the key's id is equal to the nodes id then delete it locally
		if (info.getId() == id)
			delete(info, skey, v);
		else {
			NodeInfo succ = this.findSuccessor(id);
			delete(succ, skey, v);
		}
	}

	/*
	 * Join logic.
	 */

	/*
	 * Insert this node into the DHT identified by addr.
	 */
	public void join(String host, int port) throws Failed, Invalid {
		setPred(OptNodeInfo.getDefaultInstance());
		NodeInfo info = getNodeInfo();
		NodeInfo succ;
		/*
		 * TODO: Do a web service call to the node identified by "addr" and find
		 * the successor of info.id, then setSucc(succ). Make sure to clear any
		 * local bindings first of all, to maintain consistency of the ring. We
		 * start afresh with the bindings that are transferred from the new
		 * successor.
		 * 
		 * Do a stabilize() here. Stabilize() will set the succ node's pred
		 * pointing back to us. In the case of inserting into a single-node
		 * network, there is a potential race condition: notify() will also set
		 * succ in the singleton to point back to us (a special case in
		 * notify()). If the singleton does stabilize(), it will find our pred
		 * null and perform notify() to get bindings from us. It is important
		 * that it keeps its own bindings, to which it adds those it transfers
		 * from us.
		 */


	}

	/*
	 * State display operations for CLI.
	 */

	public void display() {
		state.display();
	}

	public void routes() {
		routing.routes();
	}


	/**
	 * Streaming events: server-side
	 */

	// Webmethod
	@Override
	public void listenOn(int listenerId, String key, EventProducer eventProducer) {
		// Node "listenerId" wants to register a listener here for "key"
		int id = NodeKey(key);
		if (routing.getPred() == null ||
				inInterval(id, routing.getPred().getNodeInfo().getId(), getNodeInfo().getId())) {
			Log.debug(TAG, String.format("listenOn(%d,%s) 1", listenerId, key));
			/*
			 * TODO add the event producer as the listener to the state broadcaster
			 * (Events will be pushed to the node requesting these updates).
			 */

		} else {
			Log.debug(TAG, String.format("listenOn(%d,%s) 2", listenerId, key));
			/*
			 * TODO tell the client that they need to try again.
			 * User is trying to register a listener for a binding that has moved.
			 */

		}
	}

	// Webmethod
	@Override
	public void listenOff(int listenerId, String key) {
		Log.debug(TAG, String.format("listenOff(%d,%s) 1", listenerId, key));
		// TODO remove event output stream from broadcaster

	}

	/**
	 * Streaming events: client-side
	 */
	protected static final long PAUSE_INTERVAL = 5000;

	protected void transferListener(String key, IBindingEventListener listener) {
		/*
		 * When we are informed a binding has moved, we stop listening at the old node
		 * and start listening at the new node.  We put a pause in between to give time
		 * for routing to catch up with the moved binding, otherwise we just start listening
		 * again at the old node.
		 */
		try {
			// Stop listening at the old node.
			Log.debug(TAG, String.format("transferListener: Stopping listening for %s because the bindings have moved.", key));
			stopListening(key);
			try {
				// Wait for binding to move to new node.
				Log.debug(TAG, "transferListener: Pausing to wait for routing tables to be updated.");
				Thread.sleep(PAUSE_INTERVAL);
			} catch (InterruptedException e) {
				// https://web.archive.org/web/20210301203607/http://www.ibm.com/developerworks/java/library/j-jtp05236/index.html?ca=drs-
				Log.debug(TAG, "transferListener: Interrupted while sleeping during transfer of listener...");
				Thread.currentThread().interrupt();
			}
			// Start listening at the new node.
			Log.debug(TAG, String.format("transferListener: Restarting listening for %s after the bindings  moved.", key));
			startListening(key, listener);
		} catch (DhtBase.Failed e) {
			logger.log(Level.SEVERE, "Failed to transfer listener to new node: " + key, e);
		}
	}

	public void startListening(String key, IBindingEventListener bindingEventListener) throws DhtBase.Failed {
		/*
		 * Register a listener for new bindings under key, at the node
		 */
		int id = NodeKey(key);
		NodeInfo target = findSuccessor(id);
		Log.debug(TAG, String.format("startListening: Listener for key %s (id=%d) should be stored at node %d", key, id, target.getId()));

		// Make sure we are not already listening for bindings for that key
		if (state.startListening(key, target)) {
			Log.debug(TAG, String.format("startListening: Adding listener for %s...", key));

			NodeInfo myInfo = getNodeInfo();
			Subscription subscription = Subscription.newBuilder().setId(myInfo.getId()).setKey(key).build();

			/*
			 * WebClient will wrap this listener with an event consumer that takes
			 * the events actually pushed from the server and calls into this listener.
			 */
			IEventListener eventListener = new IEventListener() {
				@Override
				public void onNewBinding(String key, String value) {
					Log.debug(TAG, String.format("onNewBinding(%s,%s)", key, value));
					// TODO report a new binding added for key to value

				}

				@Override
				public void onMovedBinding(String key) {
					Log.debug(TAG, String.format("onMovedBinding(%s)", key));
					// TODO transfer listen notifier from previous node to new node

				}

				@Override
				public void onClosed(String key) {
					Log.debug(TAG, String.format("onClosed(%s)", key));
				}

				@Override
				public void onError(String key, Throwable t) {
					Log.debug(TAG, String.format("onError(%s)", key));
					logger.log(Level.SEVERE, "Communication error while listening on " + key, t);
					state.stopListening(key);
				}
			};

			client.listenOn(target, subscription, eventListener);

		}
	}

	public void stopListening(String key) throws DhtBase.Failed {
		/*
		 * TODO: Stop listening for new binding events for this key.  Need to
		 * do a Web service call to the server node, to stop event generation.
		 *
		 * Although the server will still send us onComplete(), we remove the
		 * local record of the listener, otherwise it would get confused with
		 * the new record if a new listener is added (e.g. after moving listener).
		 */

	}

	public void listeners(OutputStream out) {
		state.listeningFor(out);
	}



}
