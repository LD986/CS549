package edu.stevens.cs549.dht.main;

import com.google.protobuf.Empty;
import edu.stevens.cs549.dht.activity.DhtBase;
import edu.stevens.cs549.dht.rpc.Binding;
import edu.stevens.cs549.dht.rpc.Bindings;
import edu.stevens.cs549.dht.rpc.DhtServiceGrpc;
import edu.stevens.cs549.dht.rpc.Id;
import edu.stevens.cs549.dht.rpc.Key;
import edu.stevens.cs549.dht.rpc.NodeBindings;
import edu.stevens.cs549.dht.rpc.NodeInfo;
import edu.stevens.cs549.dht.rpc.OptNodeBindings;
import edu.stevens.cs549.dht.rpc.OptNodeInfo;
import edu.stevens.cs549.dht.state.IChannels;
import edu.stevens.cs549.dht.state.IState;
import io.grpc.Channel;
import io.grpc.ChannelCredentials;
import io.grpc.ClientCall;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WebClient {
	
	private static final String TAG = WebClient.class.getCanonicalName();

	private Logger logger = Logger.getLogger(TAG);

	private IChannels channels;

	private WebClient(IChannels channels) {
		this.channels = channels;
	}

	public static WebClient getInstance(IState state) {
		return new WebClient(state.getChannels());
	}

	private void error(String msg, Exception e) {
		logger.log(Level.SEVERE, msg, e);
	}

	private void info(String mesg) {
		Log.weblog(TAG, mesg);
	}

	/*
	 * Get a blocking stub (channels and stubs are cached for reuse).
	 */
	private DhtServiceGrpc.DhtServiceBlockingStub getStub(String targetHost, int targetPort) {
		Channel channel = channels.getChannel(targetHost, targetPort);
		return DhtServiceGrpc.newBlockingStub(channel);
	}

	private DhtServiceGrpc.DhtServiceBlockingStub getStub(NodeInfo target) {
		return getStub(target.getHost(), target.getPort());
	}


	/*
	 * TODO: Fill in missing operations.
	 */

	/*
	 * Get the predecessor pointer at a node.
	 */
	public OptNodeInfo getPred(NodeInfo node) {
		Log.weblog(TAG, "getPred("+node.getId()+")");
		return getStub(node).getPred(Empty.getDefaultInstance());
	}

	public NodeInfo getSucc(NodeInfo node) {
		Log.weblog(TAG, "getSucc(" + node.getId() + ")");
		return getStub(node).getSucc(Empty.getDefaultInstance());
	}

	public NodeInfo closestPrecedingFinger(NodeInfo node, int id) {
		Log.weblog(TAG, "closestPrecedingFinger(node=" + node.getId() + ", id=" + id + ")");
		Id req = Id.newBuilder().setId(id).build();
		return getStub(node).closestPrecedingFinger(req);
	}

	public NodeInfo findSuccessor(NodeInfo node, int id) {
		Log.weblog(TAG, "findSuccessor(node=" + node.getId() + ", id=" + id + ")");
		Id req = Id.newBuilder().setId(id).build();
		return getStub(node).findSuccessor(req);
	}

	public NodeInfo getNodeInfo(String host, int port) {
		Log.weblog(TAG, "getNodeInfo(" + host + ":" + port + ")");
		return getStub(host, port).getNodeInfo(Empty.getDefaultInstance());
	}

	/*
	 * Notify node that we (think we) are its predecessor.
	 */
	public OptNodeBindings notify(NodeInfo node, NodeBindings predDb) throws DhtBase.Failed {
		// TODO
		// throw new IllegalStateException("notify() not yet implemented");
		/*
		 * The protocol here is more complex than for other operations. We
		 * notify a new successor that we are its predecessor, and expect its
		 * bindings as a result. But if it fails to accept us as its predecessor
		 * (someone else has become intermediate predecessor since we found out
		 * this node is our successor i.e. race condition that we don't try to
		 * avoid because to do so is infeasible), it notifies us by returning
		 * null.
		 */
		Log.weblog(TAG, "notify(node=" + node.getId() + ", pred=" + predDb.getInfo().getId() + ")");
		try {
			return getStub(node).notify(predDb);
		}
		catch (Exception e) {
			error("notify() error", e);
			throw new DhtBase.Failed("notify() failed: " + e);
		}
	}


}
