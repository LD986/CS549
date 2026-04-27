package edu.stevens.cs549.hadoop.pagerank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRankDriver {

	/** Damping factor d used in the PageRank formula: rank = (1-d) + d*sum(incoming) */
	public static final double DECAY = 0.85;

	/**
	 * Convergence threshold: iteration stops when the maximum per-node rank
	 * difference between two consecutive iterations drops below this value.
	 */
	public static final double THRESHOLD = 30;

	public static void main(String[] args) throws Exception {

		/* ---- 1. Read the job-name argument (first positional argument) ---- */
		String job = "";
		if (args.length != 0)
			job = args[0];

		/* ---- 2. Dispatch to the appropriate job based on argument count and job name ---- */

		if (args.length == 4) {
			// 4-argument jobs: <jobName> <input> <output> <numReducers>
			if (job.equals("init")) {
				init(args[1], args[2], Integer.parseInt(args[3]));
			} else if (job.equals("iter")) {
				iter(args[1], args[2], Integer.parseInt(args[3]));
			} else if (job.equals("finish")) {
				finish(args[1], args[2], Integer.parseInt(args[3]));
			} else {
				System.err
						.println("Please check the name of the function you wish to call and try again");
			}
		} else if (args.length == 5) {
			// 5-argument jobs: <jobName> <input1> <input2> <output> <numReducers>
			if (job.equals("diff")) {
				diff(args[1], args[2], args[3], Integer.parseInt(args[4]));
			} else if (job.equals("join")) {
				join(args[1], args[2], args[3], Integer.parseInt(args[4]));
			} else {
				System.err
						.println("Please check the name of the function you wish to call and try again");
			}
		} else if (args.length == 8) {
			// 8-argument job: composite end-to-end run
			// <jobName> <input> <finalOutput> <interim1> <interim2> <namesFile> <diffDir> <numReducers>
			if (job.equals("composite")) {
				composite(args[1], // input directory containing the raw graph
						args[2],   // final output directory
						args[3],   // first interim directory (ping-pong buffer A)
						args[4],   // second interim directory (ping-pong buffer B)
						args[5],   // vertex-names file for the join step
						args[6],   // temporary directory for diff output
						Integer.parseInt(args[7])); // number of reducers
			} else {
				System.err
						.println("Please check the name of the function you wish to call and try again");
			}
		} else {
			System.err
					.println("Incorrect Usage \n Correct format: <function name><input><output><#reducers> \n Or \n <function name><input><output><diff><#reducers>"
							+ "\n Or \n <function name><input><output><interim1><interim2><diff><#reducers>");
		}
	}

	/**
	 * Initialisation job: converts the raw adjacency-list input into the
	 * "<nodeId>;<rank>\t<adjList>" format expected by subsequent iterations.
	 * Every node is assigned an initial rank of 1.
	 */
	static void init(String input, String output, int reducers)
			throws IOException, ClassNotFoundException, InterruptedException {
		System.out.println("Init Job Started");
		System.out.println("Hans Iselborn (hiselbor)");

		/* ---- 1. Remove any stale output directory to avoid conflicts ---- */
		try {
			deleteDirectory(output);
		} catch (Exception e) {
			// ignore – directory may not exist yet
		}

		/* ---- 2. Configure the Hadoop job ---- */
		Job job = Job.getInstance();
		job.setJarByClass(PageRankDriver.class);
		job.setNumReduceTasks(reducers);

		/* ---- 3. Set input / output paths ---- */
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		/* ---- 4. Set mapper and reducer classes ---- */
		job.setMapperClass(InitMapper.class);
		job.setReducerClass(InitReducer.class);

		/* ---- 5. Declare the key/value types for both map and reduce output ---- */
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		/* ---- 6. Submit and wait; report success or failure ---- */
		System.out.print(job.waitForCompletion(true) ? "Init Job Completed" : "Init Job Error");
	}

	/**
	 * Iteration job: performs one complete PageRank iteration.
	 * Reads the output of the previous step (init or a prior iter), distributes
	 * rank along outgoing edges, and writes an updated "<nodeId>;<rank>\t<adjList>"
	 * file for the next step.
	 */
	static void iter(String input, String output, int reducers)
			throws IOException, ClassNotFoundException, InterruptedException {
		System.out.println("Iter Job Started");
		System.out.println("Hans Iselborn (hiselbor)");

		/* ---- 1. Remove any stale output directory ---- */
		try {
			deleteDirectory(output);
		} catch (Exception e) {
			// ignore
		}

		/* ---- 2. Configure the Hadoop job ---- */
		Job job = Job.getInstance();
		job.setJarByClass(PageRankDriver.class);
		job.setNumReduceTasks(reducers);

		/* ---- 3. Set input / output paths ---- */
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		/* ---- 4. Set mapper and reducer classes ---- */
		job.setMapperClass(IterMapper.class);
		job.setReducerClass(IterReducer.class);

		/* ---- 5. Declare map and reduce output types ---- */
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		/* ---- 6. Submit and wait; report success or failure ---- */
		System.out.print(job.waitForCompletion(true) ? "Iter Job Completed" : "Iter Job Error");
	}

	/**
	 * Diff job: a two-stage MapReduce pipeline that measures convergence by computing
	 * the maximum absolute rank difference between two consecutive iteration outputs.
	 *
	 * Stage 1 (DiffMap1 → DiffRed1): for every node, collect both rank values and
	 *   emit the per-node absolute difference into a temporary directory "tempdiff".
	 * Stage 2 (DiffMap2 → DiffRed2): reduce all per-node differences to the single
	 *   global maximum and write it to the final output directory.
	 *
	 * The temporary "tempdiff" directory is created and deleted automatically.
	 */
	static void diff(String input1, String input2, String output, int reducers)
			throws Exception {
		System.out.println("Diff Job Part 1 Started");
		System.out.println("Hans Iselborn (hiselbor)");

		/* ---- 1. Clean up from any previous run ---- */
		deleteDirectory("tempdiff");
		deleteDirectory(output);

		/* ======== Stage 1: per-node rank difference ======== */

		/* ---- 2. Configure Stage-1 job ---- */
		Job job = Job.getInstance();
		job.setJarByClass(PageRankDriver.class);
		job.setNumReduceTasks(reducers);

		// Read from both consecutive iteration directories so DiffMap1 sees
		// two rank values per node (one from each directory)
		FileInputFormat.addInputPath(job, new Path(input1));
		FileInputFormat.addInputPath(job, new Path(input2));
		FileOutputFormat.setOutputPath(job, new Path("tempdiff"));

		job.setMapperClass(DiffMap1.class);
		job.setReducerClass(DiffRed1.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		/* ---- 3. Run Stage 1; on success, launch Stage 2 ---- */
		if (job.waitForCompletion(true)) {
			System.out.println("Diff Part 1 Complete, Part 2 Started");

			/* ======== Stage 2: global maximum difference ======== */

			/* ---- 4. Configure Stage-2 job ---- */
			Job job1 = Job.getInstance();
			job1.setJarByClass(PageRankDriver.class);
			job1.setNumReduceTasks(reducers);

			// Stage 2 reads from the temporary directory produced by Stage 1
			FileInputFormat.addInputPath(job1, new Path("tempdiff"));
			FileOutputFormat.setOutputPath(job1, new Path(output));

			job1.setMapperClass(DiffMap2.class);
			job1.setReducerClass(DiffRed2.class);

			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(Text.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);

			/* ---- 5. Run Stage 2, report result, and clean up temp directory ---- */
			System.out.print(job1.waitForCompletion(true) ? "Diff Job Completed" : "Diff Job Error");
			deleteDirectory("tempdiff");
		}
	}

	/*
	 * KeyPartitioner routes all records that share the same first TextPair field
	 * (i.e. the same node/vertex id) to the same reducer partition.  Without this,
	 * name and rank records for the same vertex could end up in different reducers
	 * and never be joined together.
	 */
	public static class KeyPartitioner extends Partitioner<TextPair, Text> {
		@Override
		public int getPartition(TextPair key, Text value, int numPartitions) {
			// Partition solely on the first field (node id), ignoring the sort tag
			return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	/**
	 * Join job: combines the vertex-names dataset with the final iteration ranks,
	 * producing (vertexName, rank) pairs for the finish step.
	 *
	 * Uses a reduce-side join with secondary sort:
	 *   - JoinNameMapper emits (nodeId, "0") → name   (tag "0" sorts first)
	 *   - JoinRankMapper emits (nodeId, "1") → rank   (tag "1" sorts second)
	 *   - KeyPartitioner groups both by nodeId into the same reducer
	 *   - TextPair.FirstComparator groups records with the same nodeId
	 *   - JoinReducer pairs the name and rank into a single output record
	 *
	 * The ranksInput directory is deleted after the job completes because the
	 * join output (stored in a temporary "joinTmp" directory) supersedes it.
	 */
	static void join(String ranksInput, String namesInput, String output, int reducers) throws Exception {
		System.out.println("Join Job Started");
		System.out.println("Hans Iselborn (hiselbor)");

		/* ---- 1. Remove stale output directory ---- */
		deleteDirectory(output);

		/* ---- 2. Configure the join job ---- */
		Job job = Job.getInstance();
		job.setJarByClass(PageRankDriver.class);
		job.setNumReduceTasks(reducers);

		// Use MultipleInputs so each dataset is processed by its own mapper class
		MultipleInputs.addInputPath(job, new Path(namesInput), TextInputFormat.class, JoinNameMapper.class);
		MultipleInputs.addInputPath(job, new Path(ranksInput), TextInputFormat.class, JoinRankMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(output));

		/* ---- 3. Set the custom partitioner and grouping comparator ---- */
		// KeyPartitioner ensures same-nodeId records go to the same reducer
		job.setPartitionerClass(KeyPartitioner.class);
		// FirstComparator groups all tags for a nodeId into one reduce() call
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);

		/* ---- 4. Declare key/value types ---- */
		job.setMapOutputKeyClass(TextPair.class);
		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(Text.class);

		/* ---- 5. Submit, wait, and clean up the now-superseded ranks directory ---- */
		System.out.print(job.waitForCompletion(true) ? "Join Job Completed" : "Join Job Error");
		deleteDirectory(ranksInput);
	}

	/**
	 * Finish job: sorts the final (vertexName, rank) pairs in descending rank order.
	 *
	 * FinMapper inverts the rank sign (emits −rank as the key) so Hadoop's default
	 * ascending sort produces a descending-rank ordering.  FinReducer then re-inverts
	 * the key to recover the true rank and emits (vertexName, rank) pairs.
	 *
	 * The input directory (which holds the join output) is deleted after the job
	 * completes since it is no longer needed.
	 */
	static void finish(String input, String output, int reducers)
			throws Exception {
		System.out.println("Finish Job Started");
		System.out.println("Hans Iselborn (hiselbor)");

		/* ---- 1. Remove stale output directory ---- */
		deleteDirectory(output);

		/* ---- 2. Configure the finish job ---- */
		Job job = Job.getInstance();
		job.setJarByClass(PageRankDriver.class);
		job.setNumReduceTasks(reducers);

		/* ---- 3. Set input / output paths ---- */
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		/* ---- 4. Set mapper and reducer classes ---- */
		job.setMapperClass(FinMapper.class);
		job.setReducerClass(FinReducer.class);

		/* ---- 5. Declare key/value types ---- */
		// Mapper emits (DoubleWritable key = −rank, Text value = vertexName)
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		// Reducer emits (Text key = vertexName, Text value = rank)
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		/* ---- 6. Submit, wait, report result, and clean up the input directory ---- */
		System.out.print(job.waitForCompletion(true) ? "Finish Job Completed" : "Finish Job Error");
		deleteDirectory(input);
	}

	/**
	 * Composite job: runs the complete PageRank pipeline end-to-end.
	 *
	 * Pipeline overview:
	 *   1. init  — assign initial ranks and build the adjacency-list records
	 *   2. iter* — repeatedly apply the PageRank formula, ping-ponging between
	 *              interim1 and interim2 to avoid re-allocating directories
	 *   3. diff  — every third iteration, compute the global max rank delta;
	 *              stop when that delta drops below THRESHOLD
	 *   4. join  — attach vertex names to the converged rank values
	 *   5. finish— sort the (name, rank) pairs in descending rank order
	 *   6. summarizeResult — write a single sorted output.txt summary file
	 *
	 * @param input     raw graph input directory
	 * @param output    final sorted output directory
	 * @param interim1  ping-pong buffer A (also holds the init output)
	 * @param interim2  ping-pong buffer B
	 * @param namesfile vertex-names file for the join step
	 * @param diffDir   temporary directory for diff output (created and deleted each time)
	 * @param reducers  number of Hadoop reduce tasks for each job
	 */
	public static void composite(String input, String output, String interim1,
								 String interim2, String namesfile, String diffDir, int reducers) throws Exception {
		System.out.println("Hans Iselborn (hiselbor)");

		int counter = 0;

		/* ---- 1. Initialisation: build adjacency-list records with rank = 1 ---- */
		init(input, interim1, reducers);
		counter++;

		/* ---- 2. Iterative refinement loop ---- */
		// Start with an arbitrarily large difference to enter the loop
		double difference = 100000000;
		int i = 0;

		while (difference >= THRESHOLD) {
			/*
			 * Ping-pong between interim1 and interim2 to avoid overwriting the
			 * current input directory while writing the new output.
			 */
			if (i % 2 == 0) {
				// Even iterations: interim1 → interim2
				iter(interim1, interim2, reducers);
			} else {
				// Odd iterations: interim2 → interim1
				iter(interim2, interim1, reducers);
			}

			/*
			 * Every third iteration, measure convergence by computing the maximum
			 * rank change between the two most recent outputs.  Using interim1 and
			 * interim2 together covers both the current and previous result.
			 */
			if (i % 3 == 0) {
				counter++;
				diff(interim1, interim2, diffDir, reducers);
				// Read the global maximum rank delta written by DiffRed2
				difference = readDiffResult(diffDir);
				System.out.println("Difference updates to:" + difference);
				// Clean up the diff output so the next diff run starts fresh
				deleteDirectory(diffDir);
			}

			/*
			 * Delete whichever interim directory was the INPUT for this iteration
			 * so it can be freely overwritten in the next iteration.
			 */
			if (i % 2 == 0) {
				deleteDirectory(interim1);  // interim1 was input on even iterations
			}
			if (i % 2 == 1) {
				deleteDirectory(interim2);  // interim2 was input on odd iterations
			}

			counter++;
			i++;
		}

		/*
		 * After the loop, one of the interim directories holds the converged ranks.
		 * Which one depends on whether i ended on an even or odd value.
		 *   - odd  i → the last iter() wrote to interim2 (interim1 was deleted)
		 *   - even i → the last iter() wrote to interim1 (interim2 was deleted)
		 */
		if (i % 2 == 1) {
			// The converged ranks are in interim2
			deleteDirectory(interim1);  // remove the now-empty other buffer
			counter++;

			// Join vertex names from namesfile with converged ranks from interim2
			String joinTmp = "joinTmp";
			deleteDirectory(joinTmp);
			join(interim2, namesfile, joinTmp, reducers);

			// Sort by descending rank and write to the final output directory
			finish(joinTmp, output, reducers);
			summarizeResult(output);
		} else {
			// The converged ranks are in interim1
			deleteDirectory(interim2);  // remove the now-empty other buffer
			counter++;

			// Join vertex names from namesfile with converged ranks from interim1
			String joinTmp = "joinTmp";
			deleteDirectory(joinTmp);
			join(interim1, namesfile, joinTmp, reducers);

			// Sort by descending rank and write to the final output directory
			finish(joinTmp, output, reducers);
			summarizeResult(output);
		}

		/* ---- 3. Report total number of MapReduce jobs submitted ---- */
		System.out.println();
		System.out.println(counter);
	}

	/**
	 * Reads up to 10 (vertexName, rank) pairs from each part-r-* output file,
	 * sorts them all in descending rank order, and writes the result to
	 * "output.txt" inside the same output directory.
	 *
	 * This provides a quick human-readable summary of the highest-ranked nodes
	 * without requiring another MapReduce job.
	 *
	 * @param path  the finish-job output directory to read and write into
	 */
	static void summarizeResult(String path) throws Exception {

		/* ---- 1. Set up HDFS access and a map to hold (name → rank) pairs ---- */
		Path finpath = new Path(path);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(path), conf);
		HashMap<String, Double> values = new HashMap();
		int size = 0;

		if (fs.exists(finpath)) {

			/* ---- 2. Read up to 10 records from each part-r-* output file ---- */
			FileStatus[] ls = fs.listStatus(finpath);
			for (FileStatus file : ls) {
				if (file.getPath().getName().startsWith("part-r-00")) {
					FSDataInputStream diffin = fs.open(file.getPath());
					BufferedReader d = new BufferedReader(new InputStreamReader(diffin));
					int i = 0;
					String diffcontent = "x";  // sentinel so the while condition is true initially
					while (i <= 10 && diffcontent != null) {
						diffcontent = d.readLine();
						if (diffcontent != null) {
							// Each line is "<name>\t<rank>"
							String[] parts = diffcontent.split("\t");
                            String node = parts[0];
							double rank = Double.parseDouble(parts[1]);
							values.put(node, rank);
							i++;
							size++;
						}
					}
					d.close();
				}
			}

			/* ---- 3. Transfer the map entries into parallel arrays for sorting ---- */
			String[] nodes = new String[size];
			Double[] ranks  = new Double[size];
			int j = 0;
			for (Map.Entry<String, Double> entry : values.entrySet()) {
				nodes[j] = entry.getKey();
				ranks[j] = entry.getValue();
				j++;
			}

			/* ---- 4. Bubble-sort the arrays in descending rank order ---- */
			// Simple O(n²) sort is acceptable here because we cap at 10×(number of part files) entries
			for (int i = 0; i < j - 1; i++) {
				for (int k = i + 1; k < j; k++) {
					if (ranks[i] < ranks[k]) {
						// Swap ranks
						double temp = ranks[i];
						ranks[i] = ranks[k];
						ranks[k] = temp;
						// Swap corresponding node names in sync
						String temps = nodes[i];
						nodes[i] = nodes[k];
						nodes[k] = temps;
					}
				}
			}

			/* ---- 5. Write the sorted results to "output.txt" in the output directory ---- */
			try {
				OutputStream os = fs.create(new Path(path + "/output.txt"));
				for (int i = 0; i < nodes.length; i++) {
					String out = nodes[i] + "\t" + ranks[i] + "\n";
					// Write character by character into the HDFS output stream
					for (int k = 0; k < out.length(); k++) {
						char c = out.charAt(k);
						os.write(c);
					}
				}
				os.close();
			} catch (IOException e) {
				System.out.println("Any Errors:");
				e.printStackTrace();
			}
		}

		System.out.println();
		System.out.println("Results Summarized");
		fs.close();
	}

	/**
	 * Reads the global maximum rank difference produced by the diff job.
	 *
	 * Scans every part-r-* file in the given directory, parses the first
	 * non-null line as a double, and returns the largest value found.
	 * This is the convergence metric used by the composite loop.
	 *
	 * @param path  directory containing DiffRed2 output files
	 * @return the maximum rank delta across all nodes in the most recent diff run
	 */
	static double readDiffResult(String path) throws Exception {
		double diffnum = 0.0;
		Path diffpath = new Path(path);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(path), conf);

		if (fs.exists(diffpath)) {
			FileStatus[] ls = fs.listStatus(diffpath);
			// Iterate over all reducer output files (supports multiple reducers)
			for (FileStatus file : ls) {
				if (file.getPath().getName().startsWith("part-r-00")) {
					FSDataInputStream diffin = fs.open(file.getPath());
					BufferedReader d = new BufferedReader(new InputStreamReader(diffin));
					String diffcontent = d.readLine();
					if (diffcontent != null) {
						double diff_temp = Double.parseDouble(diffcontent);
						// Keep the largest value across all output files
						if (diffnum < diff_temp) {
							diffnum = diff_temp;
						}
						d.close();
					}
				}
			}
		}

		fs.close();
		return diffnum;
	}

	/**
	 * Deletes a directory (and all its contents) from the Hadoop filesystem.
	 * Used to remove stale output and temporary directories before jobs run.
	 *
	 * @param path  the directory path to delete
	 */
	static void deleteDirectory(String path) throws Exception {
		Path todelete = new Path(path);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(path), conf);

		// Only attempt deletion if the directory actually exists
		if (fs.exists(todelete))
			fs.delete(todelete, true);  // true = recursive delete

		fs.close();
	}

}
