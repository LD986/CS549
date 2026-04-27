package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffRed2 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double diff_max = 0.0; // sets diff_max to a default value
		/* 
		 * TODO: Compute and emit the maximum of the differences
		 */
		// find diff_max
		for (Text v : values) {
			String s = v.toString().trim();
			if (s.isEmpty()) {
				continue;
			}
			double diff = Double.parseDouble(s);
			if (diff > diff_max) {
				diff_max = diff;
			}
		}
		// emit diff_max as key, null value
		context.write(new Text(Double.toString(diff_max)), new Text(""));

	}
}
