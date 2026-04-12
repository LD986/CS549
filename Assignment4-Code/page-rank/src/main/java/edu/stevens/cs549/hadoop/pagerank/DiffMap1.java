package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffMap1 extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		String[] sections = line.split("\t"); // Splits each line
		if (sections.length > 2) // checks for incorrect data format
		{
			throw new IOException("Incorrect data format");
		}
		/**
		 *  TODO: read node-rank pair and emit: key:node, value:rank
		 */

		if (sections.length != 2) {
			return;
		}

		String nodeRank = sections[0].trim();
		String[] nr = nodeRank.split(";");
		if (nr.length != 2) {
			throw new IOException("Incorrect data format for node;rank: " + nodeRank);
		}

		String node = nr[0].trim();
		String rank = nr[1].trim();

		if (node.isEmpty() || rank.isEmpty()) {
			return;
		}

		context.write(new Text(node), new Text(rank));

	}

}
