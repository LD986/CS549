package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

/*
 * DiffMap1 is the first map phase of the two-stage convergence-check (diff) job.
 *
 * It reads from two separate iteration output directories (interim1 and interim2)
 * and strips the adjacency list from each record, emitting only the node's rank.
 * DiffRed1 then receives the two rank values for the same node and computes their
 * absolute difference.
 *
 * Input format (from IterReducer):
 *   <nodeId>;<rank>\t<adjacencyList>
 *
 * Output:
 *   key  = node id  (Text)
 *   value= rank     (Text, numeric string)
 */
public class DiffMap1 extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {

		/* ---- 1. Convert the Hadoop record to a String and split on tab ---- */
		String line = value.toString();
		String[] sections = line.split("\t");

		/* ---- 2. Validate the two-field structure ---- */
		if (sections.length > 2) {
			throw new IOException("Incorrect data format");
		}
		if (sections.length != 2) {
			return;  // skip malformed or empty lines
		}

		/* ---- 3. Parse the "<nodeId>;<rank>" composite key (ignore the adjacency list) ---- */
		String nodeRank = sections[0].trim();
		String[] nr = nodeRank.split(";");
		if (nr.length != 2) {
			throw new IOException("Incorrect data format for node;rank: " + nodeRank);
		}

		String node = nr[0].trim();
		String rank = nr[1].trim();

		if (node.isEmpty() || rank.isEmpty()) {
			return;  // guard against empty tokens
		}

		/* ---- 4. Emit (nodeId, rank) so DiffRed1 can compare the two iteration ranks ---- */
		// DiffRed1 will receive two values for the same key (one per input directory)
		// and compute their absolute difference
		context.write(new Text(node), new Text(rank));
	}

}
