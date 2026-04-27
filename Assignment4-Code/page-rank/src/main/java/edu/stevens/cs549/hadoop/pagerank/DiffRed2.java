package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

/*
 * DiffRed2 is the second (and final) reduce phase of the convergence-check (diff) job.
 *
 * DiffMap2 routed all per-node rank differences under the single key "Difference",
 * so this reducer sees every difference value in one call and finds the maximum.
 * That global maximum is what PageRankDriver compares against THRESHOLD to decide
 * whether to keep iterating.
 *
 * Input (from DiffMap2):
 *   key  = "Difference"  (constant Text)
 *   values= all per-node absolute differences from DiffRed1
 *
 * Output:
 *   key  = maximum difference  (Text, numeric string)
 *   value= "" (empty – only the max value matters for the driver)
 */
public class DiffRed2 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		/* ---- 1. Initialise the running maximum to zero ---- */
		// Any real difference will be ≥ 0, so starting at 0.0 is a safe lower bound
		double diff_max = 0.0;

		/* ---- 2. Scan all per-node differences and track the largest one ---- */
		for (Text v : values) {
			String s = v.toString().trim();
			if (s.isEmpty()) {
				continue;  // skip empty tokens (shouldn't appear, but be defensive)
			}
			double diff = Double.parseDouble(s);
			if (diff > diff_max) {
				diff_max = diff;  // update the running maximum
			}
		}

		/* ---- 3. Emit the global maximum difference ---- */
		// PageRankDriver.readDiffResult() reads this value and compares it to THRESHOLD
		context.write(new Text(Double.toString(diff_max)), new Text(""));
	}
}
