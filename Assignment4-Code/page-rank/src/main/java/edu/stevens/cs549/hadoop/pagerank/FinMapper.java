package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

/*
 * FinMapper is the map phase of the final sort step.
 *
 * It reads the output of JoinReducer (vertex name TAB page-rank) and inverts
 * the sign of the rank so that Hadoop's default ascending sort produces a
 * descending-rank ordering in FinReducer.
 *
 * Input format (from JoinReducer):
 *   <vertexName>\t<rank>
 *
 * Output:
 *   key  = -rank  (DoubleWritable, negated so Hadoop sorts highest rank first)
 *   value= vertex name (Text)
 */
public class FinMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException, IllegalArgumentException {

		/* ---- 1. Convert the Hadoop record to a plain Java String ---- */
		String line = value.toString();

		/* ---- 2. Skip blank lines ---- */
		line = line.trim();
		if (line.isEmpty()) {
			return;
		}

		/* ---- 3. Split on tab to obtain the vertex name and its rank ---- */
		String[] parts = line.split("\t");
		if (parts.length < 2) {
			return;  // incomplete record – skip it
		}

		String name    = parts[0].trim();
		String rankStr = parts[1].trim();
		if (name.isEmpty() || rankStr.isEmpty()) {
			return;  // guard against empty tokens
		}

		/* ---- 4. Parse the rank and negate it for descending sort ---- */
		// Using a negative key causes Hadoop's ascending comparator to place
		// the highest rank first when FinReducer iterates over the results
		double rank = Double.parseDouble(rankStr);
		context.write(new DoubleWritable(-rank), new Text(name));
	}

}
