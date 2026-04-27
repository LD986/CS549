package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

/*
 * IterReducer is the reduce phase of one PageRank iteration.
 *
 * It receives all values for a single node:
 *   - Exactly one adjacency-list value (prefixed with "|" by IterMapper).
 *   - Zero or more numeric rank-contribution values (plain doubles) from nodes
 *     that link to this node.
 *
 * It applies the PageRank damping formula and emits the updated record in the
 * same "<nodeId>;<newRank>  <adjList>" format expected by the next iteration.
 *
 *   new_rank = (1 - d) + d * sum(incoming_contributions)
 *
 * where d = PageRankDriver.DECAY (0.85 by default).
 */
public class IterReducer extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		/* ---- 1. Initialise accumulators ---- */
		double d    = PageRankDriver.DECAY;  // damping factor (e.g. 0.85)
		double sum  = 0.0;                   // accumulates the rank contributions from incoming edges
		String adjacency = "";               // will hold the adjacency list once we encounter it

		/* ---- 2. Separate the adjacency-list record from numeric rank contributions ---- */
		for (Text v : values) {
			String s = v.toString().trim();
			if (s.isEmpty()) continue;

			if (s.charAt(0) == '|') {
				// The "|" prefix marks the adjacency list; strip the sentinel and save it
				adjacency = s.substring(1);
			} else {
				// All other values are rank contributions emitted by upstream nodes
				sum += Double.parseDouble(s);
			}
		}

		/* ---- 3. Apply the PageRank damping formula ---- */
		// (1 - d) is the "teleportation" base probability; d * sum is the link-following term
		double rank = (1.0 - d) + d * sum;

		/* ---- 4. Emit the updated record in the same format as the input ---- */
		// The adjacency list is forwarded unchanged so future iterations have the graph structure
		context.write(new Text(key.toString() + ";" + Double.toString(rank)), new Text(adjacency));
	}
}
