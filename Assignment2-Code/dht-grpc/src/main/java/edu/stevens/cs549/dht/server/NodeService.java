package edu.stevens.cs549.dht.server;

import com.google.protobuf.Empty;
import edu.stevens.cs549.dht.activity.Dht;
import edu.stevens.cs549.dht.activity.DhtBase.Failed;
import edu.stevens.cs549.dht.activity.DhtBase.Invalid;
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
	private Dht getDht() {
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

	@Override
	public void getPred(Empty empty, StreamObserver<OptNodeInfo> responseObserver) {
		Log.weblog(TAG, "getPred()");
		try {
			responseObserver.onNext(getDht().getPred());
			responseObserver.onCompleted();
		}
		catch (Exception e){
			error("getPred() failed", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void getSucc(Empty empty, StreamObserver<NodeInfo> responseObserver) {
		Log.weblog(TAG, "getSucc()");
		try {
			responseObserver.onNext(getDht().getSucc());
			responseObserver.onCompleted();
		}
		catch (Exception e){
			error("getSucc() failed", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void closestPrecedingFinger(Id request, StreamObserver<NodeInfo> responseObserver) {
		Log.weblog(TAG, "closestPrecedingFinger(" + request.getId() + ")");
		try {
			responseObserver.onNext(getDht().closestPrecedingFinger(request.getId()));
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			error("closestPrecedingFinger() failed", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void findSuccessor(Id request, StreamObserver<NodeInfo> responseObserver) {
		Log.weblog(TAG, "findSuccessor(" + request.getId() + ")");
		try {
			responseObserver.onNext(getDht().findSuccessor(request.getId()));
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			error("findSuccessor() error", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void getBindings(Key request, StreamObserver<Bindings> responseObserver) {
		Log.weblog(TAG, "getBindings(" + request.getKey() + ")");
		try {
			String k = request.getKey();
			String[] values = getDht().get(k); // may throw Invalid

			Bindings.Builder b = Bindings.newBuilder().setKey(k);
			if (values != null) {
				b.addAllValue(java.util.Arrays.asList(values));
			}

			responseObserver.onNext(b.build());
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			error("getBindings() error", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void addBinding(Binding request, StreamObserver<Empty> responseObserver) {
		Log.weblog(TAG, "addBinding(" + request.getKey() + "," + request.getValue() + ")");
		try {
			getDht().add(request.getKey(), request.getValue());
			responseObserver.onNext(Empty.getDefaultInstance());
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			error("addBinding() error", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void deleteBinding(Binding request, StreamObserver<Empty> responseObserver) {
		Log.weblog(TAG, "deleteBinding(" + request.getKey() + "," + request.getValue() + ")");
		try {
			getDht().delete(request.getKey(), request.getValue());
			responseObserver.onNext(Empty.getDefaultInstance());
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			error("deleteBinding() error", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void notify(NodeBindings request, StreamObserver<OptNodeBindings> responseObserver) {
		Log.weblog(TAG, "notify(from pred cand id=" + request.getInfo().getId() + ")");
		try {
			responseObserver.onNext(getDht().notify(request));
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			error("notify() error", e);
			responseObserver.onError(e);
		}
	}

}