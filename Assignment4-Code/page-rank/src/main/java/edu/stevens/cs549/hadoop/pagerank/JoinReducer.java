package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {

	public void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/* 
		 * TextPair ensures that we have values with tag "0" first, followed by tag "1"
		 * So we know that first value is the name and second value is the rank
		 */
		String k = key.toString(); // Converts the key to a String
		
		// TODO values should have the vertex name and the page rank (in that order).
		// Emit (vertex name, pagerank) or (vertex id, vertex name, pagerank)
		// Ignore if the values do not include both vertex name and page rank

		//initialize variables
		String name = null;
		String rank = null;

		// parse for name and rank
		for (Text v : values) {
			String s = v.toString();
			if (name == null) {
				name = s;
			} else {
				rank = s;
			}
		}

		if (name == null || rank == null) {
			return;
		}
		// emit as (name, rank)
		context.write(new Text(name), new Text(rank));
	}
}
