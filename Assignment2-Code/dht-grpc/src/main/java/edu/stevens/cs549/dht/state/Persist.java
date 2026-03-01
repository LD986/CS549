package edu.stevens.cs549.dht.state;

import edu.stevens.cs549.dht.activity.DhtBase;
import edu.stevens.cs549.dht.rpc.Bindings;
import edu.stevens.cs549.dht.rpc.NodeBindings;
import edu.stevens.cs549.dht.rpc.NodeInfo;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.logging.Logger;
import javax.swing.text.TableView.TableRow;

public class Persist {

	/*
	 * This class provides operations for persisting the contents of a DHT node
	 * (key-value pairs only) to disk or network.
	 * 
	 * We use JAXB for marshaling hash table contents as XML.
	 */

	protected static Logger log = Logger.getLogger("edu.stevens.cs549.dht.state.Persist");

	private static void severe(String msg) {
		log.severe(msg);
	}
	
	public static class Table extends Hashtable<String,List<String>> {
		private static final long serialVersionUID = 1L; 
	}

	public static Table newTable() {
		return new Table();
	}

 
	/********************************************************************************/
	/* Operations for mapping from TableRep to Hashtable                             */
	
	protected synchronized static void fromTableDB(Visitor v, NodeBindings db) {
		for (Bindings b : db.getBindingsList()) {
			String[] values = new String[b.getValueCount()];
			for (int ix=0; ix<b.getValueCount(); ix++) {
				values[ix] = b.getValue(ix);
			}
			v.visit(b.getKey(), values);
		}
	}

	protected static Table fromTableDBPred(NodeBindings db, int predId) {
		Table table = newTable();
		fromTableDB(new PredInsert(table, predId, db.getInfo().getId()), db);
		return table;
	}
	
	protected static Table fromTableDBSucc(NodeBindings db, int predId) {
		Table table = newTable();
		fromTableDB(new SuccInsert(table, predId, db.getInfo().getId()), db);
		return table;
	}
	
	protected static Table fromTableDBAll(NodeBindings db) {
		return fromTableDBAll(newTable(), db);
	}
	
	protected static Table fromTableDBAll(Table table, NodeBindings db) {
		fromTableDB(new AllInsert(table), db);
		return table;
	}
	
	protected static abstract class Visitor {
		/*
		 * Interface for processing a TableRep row.
		 */
		public abstract void visit (String k, String[] v);
	}
	
	protected static abstract class Insert extends Visitor {
		/*
		 * Logic for adding a TableRep row to a hash table.
		 * Relies on filter logic defined in concrete subclasses.
		 */
		protected Table table;
		public Insert(Table ht) { table=ht; }
		protected abstract boolean filter(String k);
		public void visit(String k, String[] v) {
			if (filter(k)) {
				List<String> xs = table.get(k);
				if (xs == null) {
					xs = new ArrayList<String>();
				}
				for (String s : v) {
					xs.add(s);
				}
				table.put(k, xs);
			}
		}
	}
	
	protected static class PredInsert extends Insert {
		/*
		 * Transferring bindings from successor to predecessor.
		 * Logic for including bindings that are in the predecessor.
		 */
		protected int predId, id;
		protected boolean filter(String k) {
			// return DHTBase.NodeKey(k) <= predId;
			return !DhtBase.inInterval(DhtBase.NodeKey(k), predId, id);
		}
		public PredInsert(Table ht, int predId, int id) { 
			super(ht);
			this.predId = predId;
			this.id = id;
		}
	}
	
	protected static class SuccInsert extends Insert {
		/*
		 * Transferring bindings from successor to predecessor.
		 * Logic for including bindings that are in the successor.
		 */
		protected int predId, id;
		protected boolean filter(String k) {
			// return DHTBase.NodeKey(k) > predId;
			return DhtBase.inInterval(DhtBase.NodeKey(k), predId, id);
		}
		public SuccInsert(Table ht, int predId, int id) { 
			super(ht);
			this.predId = predId;
			this.id = id;
		}
	}
	
	protected static class AllInsert extends Insert {
		protected boolean filter(String k) {
			return true;
		}
		public AllInsert(Table ht) {
			super(ht);
		}
	}
	
	/*******************************************************************************/
	/* Operations for mapping from Hashtable to TableDB                            */
	
	/*
	 * Extract bindings to NodeBindings, to be transferred to pred,
	 * so extract bindings > predId.  If predId<0, then
	 * extract all bindings.  NodeBindings also backs up succ ptr.
	 */
	protected synchronized static NodeBindings toTableDB(int predId, NodeInfo info, NodeInfo succ, Table table) {
		Enumeration<String> keys = table.keys();
		List<TableRow> rows = new ArrayList<TableRow>();

		NodeBindings.Builder nb = NodeBindings.newBuilder().setInfo(info).setSucc(succ);

		while (keys.hasMoreElements()) {
			String k = keys.nextElement();
			int id = DhtBase.NodeKey(k);
			if (predId<0 || !DhtBase.inInterval(id, predId, info.getId())) {
				/*
				 * Binding to be moved to predecessor
				 */
				nb.addBindingsBuilder().setKey(k).addAllValue(table.get(k)).build();
			}
		}

		return nb.build();
	}	
	
	protected synchronized static NodeBindings toTableDB(NodeInfo info, NodeInfo succ, Table table) {
		return toTableDB(-1, info, succ, table);
	}

	/*******************************************************************************/
	/* Applications                                                                */
	
	public static Table load(String filename) {
		try {
			InputStream is = new BufferedInputStream(new FileInputStream(filename));
			NodeBindings db = NodeBindings.parseFrom(is);
			is.close();
			return fromTableDBAll(db);
		} catch (FileNotFoundException e) {
			severe("Load: File not found: " + filename);
		} catch (IOException e) {
			severe("Load: IO Exception closing " + filename);
		}
		return null;

	}

	public static void save(NodeInfo info, NodeInfo succ, Table table, String filename) {
		try {
			OutputStream os = new BufferedOutputStream(new FileOutputStream(filename));
			NodeBindings db = toTableDB(info, succ, table);
			db.writeTo(os);
			os.close();
		} catch (FileNotFoundException e) {
			severe("Save: File not found: " + filename);
		} catch (IOException e) {
			severe("Save: IO Exception closing " + filename);
		}
	}
	
	public static String displayVals(String[] vs) {
		String vals = "{";
		if (vs.length > 0) {
			for (int i = 0; i < vs.length - 1; i++) {
				vals += vs[i];
				vals += ",";
			}
			vals += vs[vs.length - 1];
		}
		return vals+"}";
	}

	public static String displayVals(List<String> vs) {
		String vals = "{";
		if (vs.size() > 0) {
			for (int i = 0; i < vs.size() - 1; i++) {
				vals += vs.get(i);
				vals += ",";
			}
			vals += vs.get(vs.size() - 1);
		}
		return vals+"}";
	}

	public static void display(Table table, PrintWriter wr) {		
		if (table.size()==0) {
			wr.println("No entries.");
		} else {
			Enumeration<String> keys = table.keys();
			wr.printf("%9s  %2s  %s\n", "KEYSTRING", "ID", "VALUES");
			while (keys.hasMoreElements()) {
				String k = keys.nextElement();
				List<String> v = table.get(k);
				wr.printf("%9s  %2d  %s\n", k, DhtBase.NodeKey(k), displayVals(v));
			}
		}
		wr.flush();
	}
	
	public static NodeBindings extractBindings(int predId, NodeInfo info, NodeInfo succ, Table table) {
		return toTableDB(predId, info, succ, table);
	}
	
	public static NodeBindings extractBindings(NodeInfo info, NodeInfo succ, Table table) {
		return toTableDB(info, succ, table);
	}
	
	public static void dropBindings(Table table, int predId, int id) {
		/*
		 * Drop the bindings <= pred id from the successor node.
		 */
		Enumeration<String> keys = table.keys();
		while (keys.hasMoreElements()) {
			String k = keys.nextElement();
			// if (DHTBase.NodeKey(k) <= predId) {
			if (!DhtBase.inInterval(DhtBase.NodeKey(k), predId, id)) {
				table.remove(k);
			}
		}
	}
	
	public static Table installBindings(Table table, NodeBindings db) {
		/*
		 * Install bindings at predecessor with key value up to and including id.
		 */
		return fromTableDBAll(table, db);
	}

	public static Table backupBindings(NodeBindings db) {
		/*
		 * Install backup bindings at the predecessor node.
		 */
		return fromTableDBAll(db);
	}

}
