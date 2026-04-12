package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterReducer extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/* 
		 * TODO: emit key:node+rank, value: adjacency list
		 * Use PageRank algorithm to compute rank from weights contributed by incoming edges.
		 * Remember that one of the values will be marked as the adjacency list for the node.
		 */
		double d = PageRankDriver.DECAY; // Decay factor
		double rank = 0.0; // stores the decay factor in a variable rank

		String adjacency = "";

		double sum = 0.0;

		for (Text v : values) {
			String s = v.toString().trim();
			if (s.isEmpty()) continue;

			if (s.charAt(0) == '|') {
				adjacency = s.substring(1);
			} else {
				sum += Double.parseDouble(s);
			}
		}

		rank = (1.0 - d) + d * sum;

		context.write(new Text(key.toString() + ";" + Double.toString(rank)), new Text(adjacency));
	}
}
