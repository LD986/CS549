package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinRankMapper extends Mapper<LongWritable, Text, TextPair, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        /*
         * We assume join is coming after IterReducer and before FinMapper.
         */

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
         * TODO ignore the adjacency list and split the node;rank part.
         * Then emit (new TextPair(node, "1"), new Text(rank))
         */

        // parse <nodeId>;<rank> composite key
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
        // emit in proper format
        context.write(new TextPair(node, "1"), new Text(rank));
	}
}
