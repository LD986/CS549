package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinNameMapper extends Mapper<LongWritable, Text, TextPair, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String line = value.toString(); // Converts Line to a String
		String[] sections = line.split(": ", 2); // Splits it into two parts. Part 1: node | Part 2: name

		if (sections.length < 2 || sections[0].trim().isEmpty() || sections[1].trim().isEmpty()) {
			System.err.println("JoinNameMapper: skipping malformed line: " + line);
			return;
		}

		context.write(new TextPair(sections[0].trim(), "0"), new Text(sections[1].trim()));

	}
}
