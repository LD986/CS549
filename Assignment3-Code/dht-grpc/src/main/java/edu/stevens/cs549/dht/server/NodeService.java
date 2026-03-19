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
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

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
		catch (Exception e) {
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
		catch (Exception e) {
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
			error("findSuccessor() failed", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void notify(NodeBindings request, StreamObserver<OptNodeBindings> responseObserver) {
		Log.weblog(TAG, "notify(" + request.getInfo().getId() + ")");
		try {
			responseObserver.onNext(getDht().notify(request));
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			error("notify() failed", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void getBindings(Key request, StreamObserver<Bindings> responseObserver) {
		Log.weblog(TAG, "getBindings(" + request.getKey() + ")");
		try {
			String[] values = getDht().get(request.getKey());

			Bindings.Builder b = Bindings.newBuilder().setKey(request.getKey());
			if (values != null) {
				for (String v : values) {
					b.addValue(v);
				}
			}
			responseObserver.onNext(b.build());
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			error("getBindings() failed", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void addBinding(Binding request, StreamObserver<Empty> responseObserver) {
		Log.weblog(TAG, "addBinding(" + request.getKey() + ", " + request.getValue() + ")");
		try {
			getDht().add(request.getKey(), request.getValue());
			responseObserver.onNext(Empty.getDefaultInstance());
			responseObserver.onCompleted();
		}
		catch (Invalid e) {
			error("addBinding() invalid", e);
			responseObserver.onError(e);
		}
		catch (Exception e) {
			error("addBinding() failed", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void deleteBinding(Binding request, StreamObserver<Empty> responseObserver) {
		Log.weblog(TAG, "deleteBinding(" + request.getKey() + ", " + request.getValue() + ")");
		try {
			getDht().delete(request.getKey(), request.getValue());
			responseObserver.onNext(Empty.getDefaultInstance());
			responseObserver.onCompleted();
		}
		catch (Invalid e) {
			error("deleteBinding() invalid", e);
			responseObserver.onError(e);
		}
		catch (Exception e) {
			error("deleteBinding() failed", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void listenOn(Subscription request, StreamObserver<Event> responseObserver) {
		Log.weblog(TAG, "listenOn(" + request.getId() + ", " + request.getKey() + ")");
		try {
			EventProducer producer = EventProducer.create(responseObserver);
			getDht().listenOn(request.getId(), request.getKey(), producer);
		}
		catch (Exception e) {
			error("listenOn() failed", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void listenOff(Subscription request, StreamObserver<Empty> responseObserver) {
		Log.weblog(TAG, "listenOff(" + request.getId() + ", " + request.getKey() + ")");
		try {
			getDht().listenOff(request.getId(), request.getKey());
			responseObserver.onNext(Empty.getDefaultInstance());
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			error("listenOff() failed", e);
			responseObserver.onError(e);
		}
	}

	private void error(String mesg, Exception e) {
		logger.log(Level.SEVERE, mesg, e);
	}

}