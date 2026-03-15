package edu.stevens.cs549.dht.events;

import edu.stevens.cs549.dht.main.Log;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import javax.swing.text.html.HTML.Tag;

public class EventBroadcaster implements IEventBroadcaster {

    private final static String TAG = EventBroadcaster.class.getCanonicalName();
    protected Map<String,Map<Integer,EventProducer>> listeners = new HashMap<>();

    private static EventBroadcaster instance = new EventBroadcaster();

    public static EventBroadcaster getInstance() {
        return instance;
    }

    private EventBroadcaster() {
    }

    @Override
    public void addListener(int id, String key, EventProducer listener) {
        Map<Integer, EventProducer> keyListeners = listeners.computeIfAbsent(key, k -> new HashMap<>());
        keyListeners.put(id, listener);
    }

    @Override
    public void removeListener(int id, String key) {
        Map<Integer,EventProducer> keyListeners = listeners.get(key);
        if (keyListeners == null) {
            throw new IllegalStateException(String.format("Tried to remove non-existent listener 1: id=%d, key=%s.", id, key));
        }
        EventProducer listener = keyListeners.get(id);
        if (listener == null) {
            throw new IllegalStateException(String.format("Tried to remove non-existent listener 2: id=%d, key=%s.", id, key));
        }
        keyListeners.remove(id);
        listener.onClosed(key);
    }

    @Override
    public void broadcastNewBinding(String key, String value) {
        Log.debug(TAG,String.format("Notifying nodes that binding %s -> %s has been added.", key, value));
        Map<Integer,EventProducer> keyListeners = listeners.get(key);
        if (keyListeners != null) {
            for (EventProducer listener : keyListeners.values()) {
                listener.onNewBinding(key, value);
            }
        }
    }

    @Override
    public void broadcastMovedBinding(String key) {
        Log.debug(TAG, String.format("Notifying nodes that bindings for key %s have been moved to another node.", key));
        Map<Integer,EventProducer> keyListeners = listeners.get(key);
        if (keyListeners != null) {
            for (EventProducer listener : keyListeners.values()) {
                Log.debug(TAG, "Notifying a listener for "+key);
                listener.onMovedBinding(key);
            }
        }
    }
}
