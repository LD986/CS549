package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * FinReducer is the reduce phase of the final sort step.
 *
 * FinMapper emitted (−rank, vertexName) pairs.  Because Hadoop sorts keys in
 * ascending order, a lower (more negative) key means a higher actual rank, so
 * records arrive here already sorted from highest to lowest page rank.
 *
 * For each (negated) rank key, this reducer re-negates the value to recover the
 * true rank and emits one output line per vertex:
 *
 *   key  = vertex name  (Text)
 *   value= page rank    (Text, formatted as a double)
 */
public class FinReducer extends Reducer<DoubleWritable, Text, Text, Text> {

	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {

		/* ---- 1. Recover the true (positive) page rank by negating the key ---- */
		// The key was stored as -rank by FinMapper so that ascending sort == descending rank
		double rank = -key.get();

		/* ---- 2. Emit one record per vertex that shares this rank value ---- */
		for (Text v: values) {
			String name = v.toString();
			if (name == null || name.trim().isEmpty()) {
				continue;  // skip empty / null vertex names
			}
			// Output: (vertexName, rank) — the final ranked listing
			context.write(new Text(name), new Text(Double.toString(rank)));
		}
	}
}
