package edu.stevens.cs549.dht.events;

public interface IBindingEventListener {

    public void onNewBinding(String key, String value);

}
