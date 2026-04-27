package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

/*
 * JoinReducer performs the reduce side of the join between vertex names and page ranks.
 *
 * Thanks to TextPair's secondary-sort ordering (tag "0" before "1") and the custom
 * KeyPartitioner / FirstComparator, all records for a given nodeId arrive in the same
 * reduce call with the name record guaranteed to come first.
 *
 * The reducer pairs the name with the rank and emits a human-readable record:
 *
 *   key  = vertex name  (Text)
 *   value= page rank    (Text, numeric string)
 *
 * Records that are missing either the name or the rank are silently skipped.
 */
public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {

	public void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		/*
		 * TextPair's secondary sort ensures tag "0" (name) arrives before tag "1" (rank),
		 * so iterating the values in order gives us name first, then rank.
		 */

		/* ---- 1. Collect the vertex name (first value) and its rank (second value) ---- */
		String name = null;
		String rank = null;

		for (Text v : values) {
			String s = v.toString();
			if (name == null) {
				name = s;   // first value is always the vertex name (tag "0")
			} else {
				rank = s;   // second value is the page rank (tag "1")
			}
		}

		/* ---- 2. Skip if either the name or the rank is missing ---- */
		// This can happen if a nodeId appears in only one of the two input datasets
		if (name == null || rank == null) {
			return;
		}

		/* ---- 3. Emit the (vertexName, rank) pair for FinMapper to sort by rank ---- */
		context.write(new Text(name), new Text(rank));
	}
}
