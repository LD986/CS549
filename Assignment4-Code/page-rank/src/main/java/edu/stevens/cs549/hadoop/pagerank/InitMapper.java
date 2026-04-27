package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

/*
 * InitMapper reads the raw input graph (adjacency list format "nodeA: nodeB nodeC ...") and
 * emits one record per outgoing edge so that InitReducer can reassemble each node's
 * full neighbor list alongside its initial rank.
 *
 * Input format (one line per source node):
 *   <node>: <neighbor1> <neighbor2> ...
 *
 * Output:
 *   key  = source node id (Text)
 *   value= neighbor node id (Text), or "" if the node has no outgoing edges
 */
public class InitMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {

		/* ---- 1. Convert the raw Hadoop input record to a plain Java String ---- */
		String line = value.toString();

		/* ---- 2. Ignore blank / whitespace-only lines ---- */
		line = line.trim();
		if (line.isEmpty()) {
			return;
		}

		/* ---- 3. Split on ":" to separate the source node from its neighbor list ---- */
		String[] parts = line.split(":");
		if (parts.length < 1) {
			return;
		}

		String from = parts[0].trim();
		if (from.isEmpty()) {
			return;  // malformed line – no source node
		}

		/* ---- 4. Handle nodes with no outgoing edges (no RHS, or empty RHS) ---- */
		// Emit the node with an empty neighbor so it still appears in the output
		if (parts.length == 1 || parts[1].trim().isEmpty()) {
			context.write(new Text(from), new Text(""));
			return;
		}

		/* ---- 5. Parse the space-separated neighbor list and emit one record per edge ---- */
		String rhs = parts[1].trim();
		String[] tos = rhs.split("\\s+");
		for (String to : tos) {
			String t = to.trim();
			if (!t.isEmpty()) {
				// Emit (sourceNode, neighborNode) so the reducer can collect all neighbors
				context.write(new Text(from), new Text(t));
			}
		}
	}

}
