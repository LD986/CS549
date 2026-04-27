package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

/*
 * IterMapper is the map phase of one PageRank iteration.
 *
 * Input (from the previous iteration's output or from InitReducer):
 *   key  = "<nodeId>;<currentRank>"
 *   value= comma-separated adjacency list  (e.g. "7,13,99")
 *
 * For each node it emits two kinds of records:
 *   1. The adjacency list itself, tagged with a "|" prefix, so IterReducer can
 *      pass it through without confusing it with numeric rank contributions.
 *   2. One rank-contribution record per outgoing edge:
 *        key  = neighbor node id
 *        value= (currentRank / outDegree)   — the share of rank flowing to that neighbor
 */
public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {

		/* ---- 1. Convert the Hadoop record to a String and split on the tab delimiter ---- */
		String line = value.toString();
		// Each line has exactly two tab-separated fields produced by the previous reducer
		String[] sections = line.split("\t");

		/* ---- 2. Validate that the line has the expected two-field structure ---- */
		if (sections.length > 2) {
			throw new IOException("Incorrect data format");
		}
		if (sections.length != 2) {
			return;  // skip malformed or empty lines
		}

		/* ---- 3. Parse the "<nodeId>;<rank>" composite key ---- */
		String nodeRank = sections[0].trim();
		String adjList  = sections[1].trim();

		String[] nr = nodeRank.split(";");
		if (nr.length != 2) {
			throw new IOException("Incorrect data format for node;rank: " + nodeRank);
		}

		String node = nr[0].trim();
		double rank  = Double.parseDouble(nr[1].trim());

		/* ---- 4. Re-emit the adjacency list tagged with "|" so the reducer can identify it ---- */
		// The reducer must receive this node's neighbor list to forward it to the next iteration
		context.write(new Text(node), new Text("|" + adjList));

		/* ---- 5. Skip rank distribution for dangling nodes (empty adjacency list) ---- */
		if (adjList.isEmpty()) {
			return;
		}

		/* ---- 6. Count the out-degree (number of non-empty neighbors) ---- */
		String[] adjs = adjList.split(",");
		int outDegree = 0;
		for (String a : adjs) {
			if (!a.trim().isEmpty()) outDegree++;
		}
		if (outDegree == 0) {
			return;  // all entries were whitespace; treat as dangling node
		}

		/* ---- 7. Compute equal rank contribution and emit one record per outgoing edge ---- */
		// Each neighbor receives an equal share of the current node's rank
		double contrib = rank / outDegree;

		for (String a : adjs) {
			String adj = a.trim();
			if (adj.isEmpty()) continue;
			context.write(new Text(adj), new Text(Double.toString(contrib)));
		}
	}

}
