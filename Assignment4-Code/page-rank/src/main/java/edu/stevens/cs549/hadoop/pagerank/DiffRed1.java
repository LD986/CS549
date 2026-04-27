package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

/*
 * DiffRed1 is the first reduce phase of the convergence-check (diff) job.
 *
 * For each node it receives exactly two rank values — one from each of the two
 * consecutive iteration outputs — and computes their absolute difference.
 * That per-node difference is emitted for DiffRed2 to find the global maximum.
 *
 * Input (from DiffMap1):
 *   key  = node id  (Text)
 *   values= [rankFromIteration_A, rankFromIteration_B]  (exactly two Text values)
 *
 * Output:
 *   key  = absolute difference  (Text, numeric string)
 *   value= "" (empty – only the difference value matters)
 */
public class DiffRed1 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		/* ---- 1. Collect the two rank values for this node ---- */
		// We expect exactly two values: one from each input directory fed to DiffMap1
		double[] ranks = new double[2];
		int i = 0;

		for (Text v : values) {
			if (i >= 2) {
				break;  // ignore extra values if more than two appear (shouldn't happen)
			}
			String s = v.toString().trim();
			if (s.isEmpty()) {
				continue;  // skip any spurious empty values
			}
			ranks[i] = Double.parseDouble(s);
			i++;
		}

		/* ---- 2. Compute and emit the absolute per-node rank difference ---- */
		// Only emit if we received exactly two ranks; missing data means the node
		// didn't appear in both iterations (e.g., graph inconsistency – skip it)
		if (i == 2) {
			double diff = Math.abs(ranks[0] - ranks[1]);
			// The difference becomes the key so DiffMap2 can forward it to DiffRed2
			context.write(new Text(Double.toString(diff)), new Text(""));
		}
	}
}
