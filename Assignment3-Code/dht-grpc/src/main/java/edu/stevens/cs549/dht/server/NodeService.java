package edu.stevens.cs549.dht.server;

import com.google.protobuf.Empty;
import edu.stevens.cs549.dht.activity.Dht;
import edu.stevens.cs549.dht.activity.DhtBase.Failed;
import edu.stevens.cs549.dht.activity.DhtBase.Invalid;
import edu.stevens.cs549.dht.activity.IDhtNode;
import edu.stevens.cs549.dht.activity.IDhtService;
import edu.stevens.cs549.dht.events.EventProducer;
import edu.stevens.cs549.dht.main.Log;
import edu.stevens.cs549.dht.rpc.*;
import edu.stevens.cs549.dht.rpc.DhtServiceGrpc.DhtServiceImplBase;
import edu.stevens.cs549.dht.rpc.NodeInfo;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Additional resource logic.  The Web resource operations call
 * into wrapper operations here.  The main thing these operations do
 * is to call into the DHT service object, and wrap internal exceptions
 * as HTTP response codes (throwing WebApplicationException where necessary).
 * 
 * This should be merged into NodeResource, then that would be the only
 * place in the app where server-side is dependent on JAX-RS.
 * Client dependencies are in WebClient.
 * 
 * The activity (business) logic is in the dht object, which exposes
 * the IDHTResource interface to the Web service.
 */

public class NodeService extends DhtServiceImplBase {
	
	private static final String TAG = NodeService.class.getCanonicalName();
	
	private static Logger logger = Logger.getLogger(TAG);

	/**
	 * Each service request is processed by a distinct service object.
	 *
	 * Shared state is in the state object; we use the singleton pattern to make sure it is shared.
	 */
	private IDhtService getDht() {
		return Dht.getDht();
	}
	
	// TODO: add the missing operations

	private void error(String mesg, Exception e) {
		logger.log(Level.SEVERE, mesg, e);
	}

	@Override
	public void getNodeInfo(Empty empty, StreamObserver<NodeInfo> responseObserver) {
		Log.weblog(TAG, "getNodeInfo()");
		responseObserver.onNext(getDht().getNodeInfo());
		responseObserver.onCompleted();
	}


}