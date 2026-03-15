package edu.stevens.cs549.dht.state;


import edu.stevens.cs549.dht.activity.DhtBase;
import io.grpc.Channel;

public interface IChannels {

    public Channel getChannel(String targetHost, int targetPort) throws DhtBase.Failed;

    public void shutdown();

}
