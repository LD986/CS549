package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/* 
		 * TODO: Output key: node+rank, value: adjacency list
		 */

		final String rank = "1";

		StringBuilder sb = new StringBuilder();
		boolean first = true;

		for (Text v: values) {
			String s = v.toString().trim();
			if (s.isEmpty()) {
				continue;
			}
			if (!first) {
				sb.append(",");
			}
			sb.append(s);
			first = false;
		}

		Text outKey = new Text(key.toString() + ";" + rank);

		Text outVal = new Text(sb.toString());

		context.write(outKey, outVal);

	}
}
