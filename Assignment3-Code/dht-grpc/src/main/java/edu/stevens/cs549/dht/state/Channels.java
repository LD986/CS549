package edu.stevens.cs549.dht.state;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import edu.stevens.cs549.dht.activity.DhtBase;
import edu.stevens.cs549.dht.activity.DhtBase.Failed;
import edu.stevens.cs549.dht.main.Log;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class Channels implements IChannels {

    protected static final String TAG = Channels.class.getCanonicalName();

    protected static Logger logger = Logger.getLogger(TAG);

    protected HashBasedTable<String,Integer, ManagedChannel> channels = HashBasedTable.create();

    private static final Channels instance = new Channels();

    public static Channels getInstance() {
        return instance;
    }

    private ManagedChannel createChannel(String targetHost, int targetPort) {
        ChannelCredentials credentials = InsecureChannelCredentials.create();
        return Grpc.newChannelBuilderForAddress(targetHost, targetPort, credentials).build();
    }

    @Override
    public synchronized Channel getChannel(String targetHost, int targetPort) throws DhtBase.Failed {
        ManagedChannel channel = channels.get(targetHost, targetPort);
        if (channel == null || channel.isShutdown() || channel.isTerminated()) {
            try {
                channel = createChannel(targetHost, targetPort);
                Log.debug(TAG,"Creating channel with authority: "+channel.authority());
                channels.put(targetHost, targetPort, channel);
            } catch (StatusRuntimeException e) {
                throw new Failed(String.format("Failed to get connection to %s:%d", targetHost, targetPort));
            }

        }
        return channel;
    }

    @Override
    public synchronized void shutdown() {
        for (Table.Cell<String,Integer,ManagedChannel> binding : channels.cellSet()) {
            binding.getValue().shutdownNow();
        }
        for (Table.Cell<String,Integer,ManagedChannel> binding : channels.cellSet()) {
            try {
                Log.debug(TAG,"Waiting for channel to terminate: "+binding.getValue().authority());
                binding.getValue().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Log.debug(TAG, "Interruption while waiting for shutdown of channel "+binding.getValue().authority());
            }
        }
        Log.debug(TAG, "Finished waiting for channels to terminate.");
    }

}
