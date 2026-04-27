package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

/*
 * DiffMap2 is the second map phase of the convergence-check (diff) job.
 *
 * It reads the output of DiffRed1 — lines containing a single absolute-difference
 * value per node — and re-tags all of them with the same key ("Difference") so that
 * DiffRed2 can find the global maximum in a single reduce call.
 *
 * Input format (from DiffRed1):
 *   <absoluteDifference>\t""
 *
 * Output:
 *   key  = "Difference"  (Text, constant – routes all records to the same reducer)
 *   value= difference    (Text, numeric string)
 */
public class DiffMap2 extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {

		/* ---- 1. Convert the Hadoop record to a String ---- */
		String s = value.toString();

		/* ---- 2. Skip blank lines ---- */
		s = s.trim();
		if (s.isEmpty()) {
			return;
		}

		/* ---- 3. Extract the difference value from the first tab-separated field ---- */
		// DiffRed1 writes the difference as the key (first field); the value is always empty
		String[] parts = s.split("\t");
		String diff = parts[0].trim();
		if (diff.isEmpty()) {
			return;  // guard against malformed lines
		}

		/* ---- 4. Emit all differences under the same key so DiffRed2 can aggregate them ---- */
		// Using a constant key guarantees all records go to the single global-max reducer
		context.write(new Text("Difference"), new Text(diff));
	}

}
