package edu.stevens.cs549.dht.state;

import edu.stevens.cs549.dht.activity.DhtBase;
import edu.stevens.cs549.dht.events.EventBroadcaster;
import edu.stevens.cs549.dht.events.IEventBroadcaster;
import edu.stevens.cs549.dht.main.Log;
import edu.stevens.cs549.dht.rpc.NodeBindings;
import edu.stevens.cs549.dht.rpc.NodeInfo;
import edu.stevens.cs549.dht.rpc.OptNodeInfo;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import javax.swing.text.html.HTML.Tag;

/**
 * 
 * @author dduggan
 */
public class State implements IState, IRouting {

	static final long serialVersionUID = 0L;

	public static final String TAG = State.class.getCanonicalName();

	public static Logger log = Logger.getLogger(TAG);

	/*
	 * We use the singleton pattern for the state object.
	 */
	protected static State state;

	/*
	 * The identity of this network node.
	 */
	protected final NodeInfo info;

	/*
	 * Routing information
	 */
	private OptNodeInfo predecessor;

	private NodeInfo successor;

	private final NodeInfo[] finger;

	/*
	 * Communication channels.
	 */
	private final Channels channels;


	private State(NodeInfo info) {
		super();
		this.info = info;
		/*
		 * Predecessor pointer is initially null.
		 */
		this.predecessor = OptNodeInfo.getDefaultInstance();
		/*
		 * Successor pointer initially is a self-reference to the node itself.
		 */
		this.successor = info;

		this.finger = new NodeInfo[NKEYS];
		for (int i = 0; i < NKEYS; i++) {
			finger[i] = info;
		}
		/*
		 * Cache communication channels.
		 */
		channels = Channels.getInstance();
	}

	public static void initState(NodeInfo info) {
		if (state != null) {
			throw new IllegalStateException("State already initialized!");
		}
		state = new State(info);
	}

	public static IState getState() {
		if (state == null) {
			throw new IllegalStateException("Failed to initialize state!");
		}
		return state;
	}

	public Channels getChannels() {
		return channels;
	}

	/*
	 * Get the info for this DHT node.
	 */
	public NodeInfo getNodeInfo() {
		return info;
	}

	/*
	 * Local table operations.
	 */
	private Persist.Table dict = Persist.newTable();

	@SuppressWarnings("unused")
	private Persist.Table backup = Persist.newTable();

	@SuppressWarnings("unused")
	private NodeInfo backupSucc = null;

	public synchronized String[] get(String k) {
		List<String> vl = dict.get(k);
		if (vl == null) {
			return null;
		} else {
			String[] va = new String[vl.size()];
			return vl.toArray(va);
		}
	}

	public synchronized void add(String k, String v) {
		List<String> vl = dict.get(k);
		if (vl == null) {
			vl = new ArrayList<String>();
			dict.put(k, vl);
		}
		vl.add(v);
	}

	public synchronized void delete(String k, String v) {
		List<String> vs = dict.get(k);
		if (vs != null)
			vs.remove(v);
	}

	public synchronized void clear() {
		dict.clear();
	}

	/*
	 * Operations for transferring state between predecessor and successor.
	 */

	/*
	 * Successor: Extract the bindings from the successor node.
	 */
	public synchronized NodeBindings extractBindings(int predId) {
		return Persist.extractBindings(predId, info, successor, dict);
	}

	public synchronized NodeBindings extractBindings() {
		return Persist.extractBindings(info, successor, dict);
	}

	/*
	 * Successor: Drop the bindings that are transferred to the predecessor.
	 */
	public synchronized void dropBindings(int predId) {
		Persist.dropBindings(dict, predId, getNodeInfo().getId());
	}

	/*
	 * Predecessor: Install the transferred bindings.
	 */
	public synchronized void installBindings(NodeBindings db) {
		dict = Persist.installBindings(dict, db);
	}

	/*
	 * Predecessor: Back up bindings from the successor.
	 */
	public synchronized void backupBindings(NodeBindings db) {
		backup = Persist.backupBindings(db);
		// backupSucc = db.getSucc();
	}

	public synchronized void backupSucc(NodeBindings db) {
		backupSucc = db.getSucc();
	}

	/*
	 * A never-used operation for storing state in a file.
	 */
	public synchronized void backup(String filename) throws IOException {
		Persist.save(info, successor, dict, filename);
	}

	public synchronized void reload(String filename) throws IOException {
		dict = Persist.load(filename);
	}

	public synchronized void display() {
		PrintWriter wr = new PrintWriter(System.out);
		Persist.display(dict, wr);
	}


	/*
	 * Routing operations.
	 */

	public synchronized void setPred(OptNodeInfo pred) {
		predecessor = pred;
	}

	public OptNodeInfo getPred() {
		return predecessor;
	}

	public synchronized void setSucc(NodeInfo succ) {
		successor = succ;
	}

	public NodeInfo getSucc() {
		return successor;
	}

	public synchronized void setFinger(int i, NodeInfo info) {
		/*
		 * TODO: Set the ith finger.
		 */
	}

	public synchronized NodeInfo getFinger(int i) {
		/*
		 * TODO: Get the ith finger.
		 */
		return null;
	}

	public synchronized NodeInfo closestPrecedingFinger(int id) {
		/*
		 * TODO: Get closest preceding finger for id, to continue search at that
		 * node. Hint: See DHTBase.inInterval()
		 */

		return null;
	}

	public synchronized void routes() {
		PrintWriter wr = new PrintWriter(System.out);
		wr.println("Predecessor: " + predecessor);
		wr.println("Successor  : " + successor);
		wr.println("Fingers:");
		wr.printf("%7s  %3s  %s\n", "Formula", "Key", "Succ");
		wr.printf("%7s  %3s  %s\n", "-------", "---", "----");
		for (int i = 0, exp = 1; i < NFINGERS; i++, exp = 2 * exp) {
			wr.printf(" %2d+2^%1d  %3d  [id=%2d,addr=%s:%d]%n", info.getId(), i, (info.getId() + exp) % NKEYS, finger[i].getId(),
					finger[i].getHost(), finger[i].getPort());
		}
		wr.flush();
	}

	/*
	 * Server-side of listening: Who is listening, indexed by keys being listened to.
	 */
	protected IEventBroadcaster broadcaster = EventBroadcaster.getInstance();

	@Override
	public IEventBroadcaster getBroadcaster() {
		return broadcaster;
	}

	/*
	 * Client-side of listening: Remember who has the bindings we're listening on.
	 */
	protected Map<String,NodeInfo> bindingMap = new HashMap<>();

	protected ExecutorService executors = Executors.newFixedThreadPool(5);

	@Override
	public void execute(Runnable task) {
		executors.execute(task);
	}

	@Override
	public void shutdown() {
		executors.shutdown();
	}

	@Override
	public void shutdownNow() {
		executors.shutdownNow();
	}

	@Override
	public void awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		executors.awaitTermination(timeout, unit);
	}

	@Override
	public boolean startListening(String key, NodeInfo target) {
		Log.debug(TAG, String.format("startListening: We are adding a listener for %s on %s", key, target.getId()));
		if (bindingMap.containsKey(key)) {
			Log.debug(TAG, "....listener already defined!");
			return false;
		}
		bindingMap.put(key, target);
		return true;
	}

	@Override
	public NodeInfo getListeningTarget(String key) {
		return bindingMap.get(key);
	}

	@Override
	public void stopListening(String key) {
		Log.debug(TAG, "stopListening: Removing record that we are listening on "+key);
		bindingMap.remove(key);
		Log.debug(TAG, String.format("stopListening: bindingMap.get(%s)="+bindingMap.get(key), key));
	}

	@Override
	public void listeningFor(OutputStream out) {
		PrintWriter wr = new PrintWriter(System.out);
		if (bindingMap.isEmpty()) {
			wr.println("No listeners defined.");
		} else {
			wr.println("Listeners defined for:");
			for (Entry<String, NodeInfo> entry : bindingMap.entrySet()) {
				wr.println(entry.getKey());
			}
		}
		wr.flush();
	}

}
