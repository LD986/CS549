package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		/* 
		 * TODO: Just echo the input, since it is already in adjacency list format.
		 * Alternatively, output adjacency pairs that will be collected by reducer.
		 */

		line = line.trim();
		if (line.isEmpty()) {
			return;
		}

		String[] parts = line.split(":");
		if (parts.length < 1) {
			return;
		}

		String from = parts[0].trim();
		if (from.isEmpty()) {
			return;
		}

		if (parts.length == 1) {
			return;
		}

		String rhs = parts[1].trim();
		if (rhs.isEmpty()) {
			return;
		}
`
		String[] tos = rhs.split("\\s+");
		for (String to : tos) {
			String t = to.trim();
			if (!t.isEmpty()) {
				context.write(new Text(from), new Text(t));
			}
		}
	}

}
