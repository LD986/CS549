package edu.stevens.cs549.dht.events;

import edu.stevens.cs549.dht.rpc.Binding;
import edu.stevens.cs549.dht.rpc.Event;
import io.grpc.stub.StreamObserver;

public class EventConsumer implements StreamObserver<Event> {

    /*
     * Wrap the consumption of streamed gRPC events with the EventListener interface.
     */

    protected String key;

    protected IEventListener eventListener;

    protected EventConsumer(String key, IEventListener listener) {
        this.key = key;
        this.eventListener = listener;
    }

    public static EventConsumer create(String key, IEventListener listener) {
        return new EventConsumer(key, listener);
    }

    @Override
    public void onNext(Event event) {
        if (event.hasNewBinding()) {
            Binding binding = event.getNewBinding();
            eventListener.onNewBinding(binding.getKey(), binding.getValue());
        } else if (event.hasMovedBinding()) {
            eventListener.onMovedBinding(key);
        } else {
            throw new IllegalStateException(("Unexpected case for listener event!"));
        }
    }

    @Override
    public void onError(Throwable throwable) {
        eventListener.onError(key, throwable);
    }

    @Override
    public void onCompleted() {
        eventListener.onClosed(key);
    }

}
