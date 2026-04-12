package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class FinMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException, IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		/*
		 * TODO output key:-rank, value: node
		 *
		 * IF NOT DOING JOIN:
		 * See IterMapper for hints on parsing the output of IterReducer.
		 *
		 * IF DOING JOIN:
		 * Instead of reading the output of IterReducer, you are reading the output of JoinReducer.
		 */

		line = line.trim();
		if (line.isEmpty()) {
			return;
		}

		String[] parts = line.split("\t");
		if (parts.length < 2) {
			return;
		}

		String name = parts[0].trim();
		String rankStr = parts[1].trim();
		if (name.isEmpty() || rankStr.isEmpty()) {
			return;
		}

		double rank = Double.parseDouble(rankStr);

		context.write(new DoubleWritable(-rank), new Text(name));
	}

}
