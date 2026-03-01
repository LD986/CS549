package edu.stevens.cs549.dht.state;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import edu.stevens.cs549.dht.main.Log;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Channel;
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
    public synchronized Channel getChannel(String targetHost, int targetPort) {
        ManagedChannel channel = channels.get(targetHost, targetPort);
        if (channel == null || channel.isShutdown() || channel.isTerminated()) {
            channel = createChannel(targetHost, targetPort);
            Log.weblog(TAG, "Creating channel with authority: "+channel.authority());
            channels.put(targetHost, targetPort, channel);
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
                logger.info("Waiting for channel to terminate: "+binding.getValue().authority());
                binding.getValue().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.info("Interruption while waiting for shutdown of channel "+binding.getValue().authority());
            }
        }
        logger.info("Finished waiting for channels to terminate.");
    }

}
