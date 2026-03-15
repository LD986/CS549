/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.stevens.cs549.dht.main;

import edu.stevens.cs549.dht.activity.Dht;
import edu.stevens.cs549.dht.activity.IDhtBackground;
import edu.stevens.cs549.dht.activity.IDhtNode;
import edu.stevens.cs549.dht.events.IBindingEventListener;
import edu.stevens.cs549.dht.rpc.NodeInfo;
import edu.stevens.cs549.dht.state.Persist;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.logging.Logger;

/*
 * CLI for a DHT node.
 */

public class CliClient {

	public static Logger log = Logger.getLogger(CliClient.class.getCanonicalName());

	protected IDhtNode node;
	

	protected App app;
	
	protected long key;
	
	public CliClient(NodeInfo info, App m) {
		node = Dht.getDht();
		app = m;
		key = info.getId();
	}

	protected void msg(String m) {
		System.out.print(m);;
	}

	protected void msgln(String m) {
		System.out.println(m);
		System.out.flush();
	}

	protected void err(Exception e) {
		app.severe("Error : " + e);
		e.printStackTrace();
	}

	protected OutputStream getOutput() {
		return System.out;
	}
	
	public IDhtBackground getDHT() {
		return (IDhtBackground)node;
	}

	public void cli() {

		// Main command-line interface loop

		Dispatch d = new Dispatch(node);
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

		try {
			while (true) {
				msg("dht<" + key + "> ");
				String line = in.readLine();
				String[] inputs = line.split("\\s+");
				if (inputs.length > 0) {
					String cmd = inputs[0];
					if (cmd.length() == 0)
						;
					else if ("get".equals(cmd))
						d.get(inputs);
					else if ("add".equals(cmd))
						d.add(inputs);
					else if ("del".equals(cmd))
						d.delete(inputs);
					else if ("bindings".equals(cmd))
						d.bindings(inputs);
					else if ("join".equals(cmd))
						d.join(inputs);
					else if ("routes".equals(cmd))
						d.routes(inputs);
					else if ("silent".equals(cmd))
						d.background(inputs);
					else if ("debug".equals(cmd))
						d.debug(inputs);
					else if ("weblog".equals(cmd))
						d.weblog(inputs);
					else if ("listenOn".equals(cmd))
						d.listenOn(inputs);
					else if ("listenOff".equals(cmd))
						d.listenOff(inputs);
					else if ("listeners".equals(cmd))
						d.listeners(inputs);
					else if ("help".equals(cmd))
						d.help(inputs);
					else if ("quit".equals(cmd))
						return;
					else
						msgln("Bad input.  Type \"help\" for more information.");
				}
			}
		} catch (EOFException e) {
		} catch (IOException e) {
			err(e);
			System.exit(-1);
		}

	}
	
	protected class Dispatch implements IBindingEventListener {

		protected IDhtNode node;
		
		protected 

		Dispatch(IDhtNode n) {
			node = n;
		}

		public void help(String[] inputs) {
			if (inputs.length == 1) {
				msgln("Commands are:");
				// Network-wide commands
				msgln("  get key: get values under a key");
				msgln("  add key value: add a value under a key");
				msgln("  del key value: delete a value under a key");
				// Network protocol
				msgln("  join host port: join a DHT as a new node");
				msgln("  bindings: display all key-value bindings");
				msgln("  routes: display routing information");
				// Logging commands.
				msgln("  silent: toggle on and off logging of background processing");
				msgln("  weblog: toggle on and off web service logging");
				msgln("  debug: toggle on and off debug logging");
				// Listening commands.
				msgln("  listenOn key: request notification of a change in binding for this key");
				msgln("  listenOff key: disable any further notifications for this key");
				msgln("  listeners: keys for which listeners are defined");

				msgln("  quit: exit the client");
			}
		}

		public void get(String[] inputs) {
			if (inputs.length == 2)
				try {
					String[] vs = node.getNet(inputs[1]);
					if (vs != null)
						msgln(Persist.displayVals(vs));
				} catch (Exception e) {
					err(e);
				}
			else
				msgln("Usage: get <key>");
		}

		public void add(String[] inputs) {
			if (inputs.length == 3)
				try {
					node.addNet(inputs[1], inputs[2]);
				} catch (Exception e) {
					err(e);
				}
			else
				msgln("Usage: add <key> <value>");
		}

		public void delete(String[] inputs) {
			if (inputs.length == 3)
				try {
					node.deleteNet(inputs[1], inputs[2]);
				} catch (Exception e) {
					err(e);
				}
			else
				msgln("Usage: del <key> <value>");
		}

		public void bindings(String[] inputs) {
			if (inputs.length == 1)
				try {
					node.display();
				} catch (Exception e) {
					err(e);
				}
			else
				msgln("Usage: bindings");
		}

		public void routes(String[] inputs) {
			if (inputs.length == 1)
				try {
					node.routes();
				} catch (Exception e) {
					err(e);
				}
			else
				msgln("Usage: routes");
		}

		public void background(String[] inputs) {
			if (inputs.length == 1)
				try {
					Log.setBackground();
				} catch (Exception e) {
					err(e);
				}
			else
				msgln("Usage: silent");
		}

		public void debug(String[] inputs) {
			if (inputs.length == 1)
				try {
					Log.setDebug();
				} catch (Exception e) {
					err(e);
				}
			else
				msgln("Usage: debug");
		}

		public void weblog(String[] inputs) {
			if (inputs.length == 1)
				try {
					Log.setLogging();
				} catch (Exception e) {
					err(e);
				}
			else
				msgln("Usage: weblog");
		}

		public void join(String[] inputs) {
			if (inputs.length == 3)
				try {
					node.join(inputs[1], Integer.parseInt(inputs[2]));
				} catch (NumberFormatException e) {
					msgln("Malformed port number!");
				} catch (Exception e) {
					err(e);
				}
			else
				msgln("Usage: join <host> <port>");
		}

		public void listenOn(String[] inputs) {
			if (inputs.length == 2)
				try {
					node.startListening(inputs[1], this);
				} catch (Exception e) {
					err(e);
				}
			else
				msgln("Usage: listenOn <key>");
		}

		public void listenOff(String[] inputs) {
			if (inputs.length == 2)
				try {
					node.stopListening(inputs[1]);
				} catch (Exception e) {
					err(e);
				}
			else
				msgln("Usage: listenOff <key>");
		}

		public void listeners(String[] inputs) {
			if (inputs.length == 1)
				try {
					node.listeners(getOutput());
				} catch (Exception e) {
					err(e);
				}
			else
				msgln("Usage: listeners");
		}

		@Override
		public void onNewBinding(String key, String value) {
			msgln(String.format("** Binding added: %s -> %s", key, value));
		}

	}

}
