package edu.stevens.cs549.dht.events;

import com.google.protobuf.Empty;
import edu.stevens.cs549.dht.rpc.Binding;
import edu.stevens.cs549.dht.rpc.Event;
import io.grpc.stub.StreamObserver;

public class EventProducer implements IEventListener {

    /*
     * Wrap the production of streamed gRPC events with the EventListener interface.
     */

    private StreamObserver<Event> observer;

    private EventProducer(StreamObserver<Event> observer) {
        this.observer = observer;
    }

    public static EventProducer create(StreamObserver<Event> observer) {
        return new EventProducer(observer);
    }

    @Override
    public void onNewBinding(String key, String value) {
        // TODO emit new binding event to listening client.
        Binding binding = Binding.newBuilder()
                .setKey(key)
                .setValue(value)
                .build();

        Event event = Event.newBuilder()
                .setNewBinding(binding)
                .build();

        observer.onNext(event);
    }

    @Override
    public void onMovedBinding(String key) {
        // TODO emit moved binding event to listening client.

        Event event = Event.newBuilder()
                .setMovedBinding(Empty.getDefaultInstance())
                .build();

        observer.onNext(event);
    }

    @Override
    public void onClosed(String key) {
        observer.onCompleted();
    }

    @Override
    public void onError(String key, Throwable throwable) {
        observer.onError(throwable);
    }

}
