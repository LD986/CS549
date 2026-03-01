package edu.stevens.cs549.dht.main;

import edu.stevens.cs549.dht.activity.Background;
import edu.stevens.cs549.dht.activity.Dht;
import edu.stevens.cs549.dht.server.DhtServer;
import edu.stevens.cs549.dht.rpc.NodeInfo;
import edu.stevens.cs549.dht.state.IRouting;
import edu.stevens.cs549.dht.state.State;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

public class App {

	public static final String serverPropsFile = "/server.properties";
	public static final String loggerPropsFile = "/log4j.properties";

	private static Logger log = Logger.getLogger(App.class.getCanonicalName());

	public void severe(String s) {
		log.severe(s);
	}

	public void severe(String s, Throwable e) {
		log.log(Level.SEVERE, s, e);
	}

	public void warning(String s) {
		log.info(s);
	}

	public void info(String s) {
		log.info(s);
	}

	/*
	 * Hostname and port for HTTP server URL.
	 */
	private static String host;

	private static int httpPort;

	/*
	 * The key for our place in the Chord ring. We will set it to a random id,
	 * and then set if a value was specified on the command line.
	 */
	private static int nodeId;
	
	protected static NodeInfo INFO;

	private App(String[] args) {

		try {
			/*
			 * Load server properties.
			 */
			Properties props = new Properties();
			InputStream in = getClass().getResourceAsStream(serverPropsFile);
			props.load(in);
			in.close();

			httpPort = Integer.parseInt((String) props.getProperty("server.port.http", "8080"));

			host = (String) props.getProperty("server.host", InetAddress.getLocalHost().getHostName());

			nodeId = new Random().nextInt(IRouting.NKEYS);

			/*
			 * Properties may be overridden by command line options.
			 */

			processArgs(args);

			INFO = NodeInfo.newBuilder().setId(nodeId).setHost(host).setPort(httpPort).build();

			State.initState(INFO);

		} catch (java.io.FileNotFoundException e) {
			severe("Server error: " + serverPropsFile + " file not found.");
		} catch (java.io.IOException e) {
			severe("Server error: IO exception.");
		} catch (Exception e) {
			severe("Server exception:" + e);
		}
	}

	protected List<String> processArgs(String[] args) {
		List<String> commandLineArgs = new ArrayList<String>();
		int ix = 0;
		Hashtable<String, String> opts = new Hashtable<String, String>();

		while (ix < args.length) {
			if (args[ix].startsWith("--")) {
				String option = args[ix++].substring(2);
				if (ix == args.length || args[ix].startsWith("--"))
					severe("Missing argument for --" + option + " option.");
				else if (opts.containsKey(option))
					severe("Option \"" + option + "\" already set.");
				else
					opts.put(option, args[ix++]);
			} else {
				commandLineArgs.add(args[ix++]);
			}
		}
		/*
		 * Overrides of values from configuration file.
		 */
		Enumeration<String> keys = opts.keys();
		while (keys.hasMoreElements()) {
			String k = keys.nextElement();
			if ("host".equals(k))
				host = opts.get("host");
			else if ("http".equals(k))
				httpPort = Integer.parseInt(opts.get("http"));
			else if ("id".equals(k))
				nodeId = Integer.parseInt(opts.get("id"));
			else
				severe("Unrecognized option: --" + k);
		}

		return commandLineArgs;
	}

	protected boolean terminated = false;

	public void setTerminated() {
		terminated = true;
	}

	public boolean isTerminated() {
		return terminated;
	}

	public static void main(String[] args) throws IOException {

		App app = new App(args);

		/*
		 * Start the HTTP server (for Web services) and background thread for
		 * stabilize.
		 */

		DhtServer dhtServer = DhtServer.createServer(httpPort);
		dhtServer.start();
		app.info(String.format("Server started on port %d (ID=%d)", httpPort, nodeId));

		Thread t = new Thread(new Background(5000, 8, app, Dht.getDht()));
		t.start();
		app.info("Background stabilization thread running.");

		/*
		 * Start the command-line loop.
		 */
		CliClient client = new CliClient(INFO, app);
		client.cli();

		/*
		 * Execute when the CLI terminates.
		 */
		app.info("Terminating background processing...");
		app.setTerminated();
		app.info("Shutting down server...");
		try {
			dhtServer.stop();
		} catch (InterruptedException e) {
			log.log(Level.SEVERE, "Interrupted while shutting down server!", e);
		}

	}
}
