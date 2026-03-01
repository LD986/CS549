package edu.stevens.cs549.dht.state;


import io.grpc.Channel;

public interface IChannels {

    public Channel getChannel(String targetHost, int targetPort);

    public void shutdown();

}
