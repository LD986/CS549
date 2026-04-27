package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

/*
 * InitReducer collects all neighbor entries for a single source node that were emitted
 * by InitMapper and assembles them into the canonical record format used by every
 * subsequent MapReduce step:
 *
 *   key  = "<nodeId>;<initialRank>"   (e.g. "42;1")
 *   value= comma-separated adjacency list  (e.g. "7,13,99")
 *
 * Every node starts with rank = 1.  Dangling nodes (no outgoing edges) get an
 * empty adjacency-list string.
 */
public class InitReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		/* ---- 1. Every node begins with an initial PageRank of 1 ---- */
		final String rank = "1";

		/* ---- 2. Collect all neighbor node ids into a comma-separated list ---- */
		StringBuilder sb = new StringBuilder();
		boolean first = true;

		for (Text v: values) {
			String s = v.toString().trim();
			if (s.isEmpty()) {
				// Skip the empty-string sentinel emitted for nodes with no outgoing edges
				continue;
			}
			if (!first) {
				sb.append(",");  // separate successive neighbor entries with a comma
			}
			sb.append(s);
			first = false;
		}

		/* ---- 3. Build the composite output key "<nodeId>;<rank>" ---- */
		// Encoding rank in the key lets IterMapper extract it without an extra field
		Text outKey = new Text(key.toString() + ";" + rank);

		/* ---- 4. The adjacency list becomes the record value ---- */
		Text outVal = new Text(sb.toString());

		context.write(outKey, outVal);
	}
}
