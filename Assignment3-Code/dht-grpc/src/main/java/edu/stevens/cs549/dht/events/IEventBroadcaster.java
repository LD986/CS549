package edu.stevens.cs549.dht.events;

public interface IEventBroadcaster {

    public void addListener(int id, String key, EventProducer listener);

    public void removeListener(int id, String key);

    public void broadcastNewBinding(String key, String value);

    public void broadcastMovedBinding(String key);

}
