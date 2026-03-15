package edu.stevens.cs549.dht.events;

public interface IEventListener {
    /*
     * A new binding has been added for the key.
     */
    public void onNewBinding(String key, String value);

    /*
     * The bindings for the key have moved to another node that has joined the network.
     */
    public void onMovedBinding(String key);

    /*
     * If the server closes the event stream to the client.
     */
    public void onClosed(String key);

    /*
     * Problem with communication link.
     */
    public void onError(String key, Throwable throwable);
}
