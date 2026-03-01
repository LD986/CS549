package edu.stevens.cs549.dht.server;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerCredentials;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class DhtServer {

    private static final Logger logger = Logger.getLogger(DhtServer.class.getCanonicalName());

    private final int port;

    private final Server server;

    public static final String getServerAddress(String host, int port) {
        return String.format("%s:%d", host, port);
    }

    public static DhtServer createServer(int port) throws IOException {
        return new DhtServer(port);
    }

    private DhtServer(int port) throws IOException {
        this.port = port;

        ServerCredentials credentials = InsecureServerCredentials.create();
        this.server = Grpc.newServerBuilderForPort(port, credentials)
                .addService(new NodeService())
                .build();
    }


    /** Start serving requests. */
    public void start() throws IOException {
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    DhtServer.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
            }
        });
    }

    /** Stop serving requests and shutdown resources. */
    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
}
