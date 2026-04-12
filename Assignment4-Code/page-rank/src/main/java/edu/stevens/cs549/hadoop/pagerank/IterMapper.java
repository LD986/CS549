package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		String[] sections = line.split("\t"); // Splits it into two parts. Part 1: node;rank | Part 2: adj list

		if (sections.length > 2) // Checks if the data is in the incorrect format
		{
			throw new IOException("Incorrect data format");
		}
		if (sections.length != 2) {
			return;
		}
		
		/* 
		 * TODO: emit key: adj vertex, value: computed weight.
		 * 
		 * Remember to also emit the input adjacency list for this node!
		 * Put a marker on the string value to indicate it is an adjacency list.
		 */

		String nodeRank = sections[0].trim();
		String adjList = sections[1].trim();

		String[] nr = nodeRank.split(";");
		if (nr.length != 2) {
			throw new IOException("Incorrect data format for node;rank: " + nodeRank);
		}

		String node = nr[0].trim();
		double rank = Double.parseDouble(nr[1].trim());

		context.write(new Text(node), new Text("|" + adjList));

		if (adjList.isEmpty()) {
			return;
		}

		String[] adjs = adjList.split(",");
		int outDegree = 0;
		for (String a : adjs) {
			if (!a.trim().isEmpty()) outDegree++;
		}
		if (outDegree == 0) {
			return;
		}

		double contrib = rank / outDegree;

		for (String a : adjs) {
			String adj = a.trim();
			if (adj.isEmpty()) continue;
			context.write(new Text(adj), new Text(Double.toString(contrib)));
		}

	}

}
