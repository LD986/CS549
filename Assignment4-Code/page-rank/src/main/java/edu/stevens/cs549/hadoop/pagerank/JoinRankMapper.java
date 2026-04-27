package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * JoinRankMapper is the second of two map phases in the join step.
 *
 * It reads the final iteration output (format: "<nodeId>;<rank>\t<adjacencyList>")
 * and emits only the node id and its rank, ignoring the adjacency list which is no
 * longer needed at this stage.  Records are tagged "1" so they sort after the name
 * records (tagged "0") emitted by JoinNameMapper within the same nodeId group.
 *
 * Input format (from IterReducer):
 *   <nodeId>;<rank>\t<adjacencyList>
 *
 * Output:
 *   key  = TextPair(nodeId, "1")  — tag "1" orders ranks after names
 *   value= rank (Text, numeric string)
 */
public class JoinRankMapper extends Mapper<LongWritable, Text, TextPair, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		/*
		 * Join is positioned after the last IterReducer and before FinMapper.
		 * We only need the (nodeId, rank) pair; the adjacency list is discarded here.
		 */

		/* ---- 1. Convert the Hadoop record to a String and split on the tab delimiter ---- */
		String line = value.toString();
		String[] sections = line.split("\t");

		/* ---- 2. Validate the two-field structure ---- */
		if (sections.length > 2) {
			throw new IOException("Incorrect data format");
		}
		if (sections.length != 2) {
			return;  // skip malformed or empty lines
		}

		/* ---- 3. Parse the "<nodeId>;<rank>" composite key, ignoring the adjacency list ---- */
		String nodeRank = sections[0].trim();
		String[] nr = nodeRank.split(";");
		if (nr.length != 2) {
			throw new IOException("Incorrect data format for node;rank: " + nodeRank);
		}

		String node = nr[0].trim();
		String rank = nr[1].trim();

		if (node.isEmpty() || rank.isEmpty()) {
			return;  // guard against empty tokens
		}

		/* ---- 4. Emit (nodeId, "1") -> rank so rank arrives second in JoinReducer ---- */
		// Tag "1" > "0" ensures ranks arrive after the corresponding vertex name
		context.write(new TextPair(node, "1"), new Text(rank));
	}
}
