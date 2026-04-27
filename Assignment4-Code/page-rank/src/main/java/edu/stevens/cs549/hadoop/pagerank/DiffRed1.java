package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffRed1 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double[] ranks = new double[2];
		/* 
		 * TODO: The list of values should contain two ranks.  Compute and output their difference.
		 */
		// extract ranks, ignoring improperly formatted lines
		int i = 0;
		for(Text v : values) {
			if (i >= 2) {
				break;
			}
			String s = v.toString().trim();
			if (s.isEmpty()) {
				continue;
			}
			ranks[i] = Double.parseDouble(s);
			i++;
		}

		// calculate absolute difference and emit as key, null value
		if (i == 2) {
			double diff = Math.abs(ranks[0] - ranks[1]);
			context.write(new Text(Double.toString(diff)), new Text(""));
		}
	}
}
