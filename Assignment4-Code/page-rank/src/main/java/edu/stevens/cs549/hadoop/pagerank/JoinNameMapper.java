package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * JoinNameMapper is one of two map phases in the join step.
 *
 * It reads the vertex-names file (format: "<nodeId>: <vertexName>") and emits
 * each record with a composite key of (nodeId, "0").  The "0" tag ensures that
 * name records sort before rank records (tag "1") within the same nodeId group,
 * so JoinReducer always sees the name first.
 *
 * Input format (names file):
 *   <nodeId>: <vertexName>
 *
 * Output:
 *   key  = TextPair(nodeId, "0")  — tag "0" orders names before ranks
 *   value= vertex name (Text)
 */
public class JoinNameMapper extends Mapper<LongWritable, Text, TextPair, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		/* ---- 1. Convert the Hadoop record to a String and split on ": " ---- */
		String line = value.toString();
		// The names file uses "nodeId: name" format (colon followed by a space)
		String[] sections = line.split(": ");

		/* ---- 2. Emit (nodeId, "0") -> name so name arrives first in JoinReducer ---- */
		// Tag "0" < "1", so TextPair's natural ordering guarantees names precede ranks
		context.write(new TextPair(sections[0], "0"), new Text(sections[1]));
	}
}
