package org.apache.hadoop.BigDataProject;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.FileSystems;
import java.nio.file.Files;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;


public class Driver extends BaseJob {

	// counter to determine the number of iterations or if more iterations are
	// required to execute the map and reduce functions


	public static class MapperCaller extends MapperWorker {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			Node inNode = new Node(value.toString());

			// calls the map method of the super class SearchMapper
			super.map(key, value, context, inNode);

		}
	}


	// the type parameters are the input keys type, the input values type, the
	// output keys type, the output values type

	public static class ReducerCaller extends ReducerWorker {

		// the parameters are the types of the input key, the values associated
		// with the key and the Context object through which the Reducer
		// communicates with the Hadoop framework

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// create a new out node and set its values
			Node outNode = new Node();

			// call the reduce method of SearchReducer class
			outNode = super.reduce(key, values, context, outNode);


		}
	}

	// method to set the configuration for the job and the mapper and the
	// reducer classes
	private Job getJobConf(String[] args) throws Exception {

		JobInfo jobInfo = new JobInfo() {

			@Override
			public Class<?> getJarByClass() {
				return Driver.class;
			}

			@Override
			public Class<? extends Mapper> getMapperClass() {
				return MapperCaller.class;
			}

			@Override
			public Class<?> getOutputKeyClass() {
				return Text.class;
			}

			@Override
			public Class<?> getOutputValueClass() {
				return Text.class;
			}

			@Override
			public Class<? extends Reducer> getReducerClass() {
				return ReducerCaller.class;
			}
		};

		return setupJob("driver", jobInfo);

	}

	// the driver to execute the job and invoke the map/reduce functions
	public int run(String[] args) throws Exception {
		
		// counter to set the ordinal number of the intermediate outputs
		int iterationCount = 0; 
		
		// setting variable for convergence value
		boolean termination = false; 
		Job job;

		// while until number of iteration is less than value or the program converges
		while (iterationCount <= 100 && !termination) {

			job = getJobConf(args); // get the job configuration
			String input, output;

			// setting the input file and output file for each iteration
			// during the first time the user-specified file will be the input
			// whereas for the subsequent iterations
			// the output of the previous iteration will be the input
			if (iterationCount == 0) { // for the first iteration the input will
										// be the first input argument
				input = "input";
			} else {
				// for the remaining iterations, the input will be the output of
				// the previous iteration
				input = "output/output" + iterationCount;
			}

			// merge all reducer output to check convergence
			String srcPath = input;
			String dstPath = "mergedOutput/output" + iterationCount;
			Configuration conf = new Configuration();
			try {
				FileSystem hdfs = FileSystem.get(conf);
				FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, conf, null);
			} catch (IOException e) {
			}

			// comparing last output with the previous
			if (iterationCount >= 2) {

				java.nio.file.Path before = FileSystems.getDefault().getPath("mergedOutput/output" + (iterationCount - 1));
				java.nio.file.Path now = FileSystems.getDefault().getPath("mergedOutput/output" + iterationCount);

				//calling the comparation function compareFiles()
				termination = compareFiles(before, now);

			}

			// setting the output file
			output = "output/output" + (iterationCount + 1); 

			 // setting the input files for the job
			FileInputFormat.setInputPaths(job, new Path(input));
			
			// setting the output files for the job
			FileOutputFormat.setOutputPath(job, new Path(output)); 

			// wait for the job to complete
			job.waitForCompletion(true); 

			iterationCount++;

		}

		return 0;
	}

	// comparing two files 
	static final boolean compareFiles(final java.nio.file.Path filea, final java.nio.file.Path fileb)
			throws IOException {
		
		// comparing file size
		if (Files.size(filea) != Files.size(fileb)) {
			return false;
		}

		final long size = Files.size(filea);
		
		// setting the size of the portion file compared
		final int mapspan = 4 * 1024 * 1024;

		
		try (FileChannel chana = (FileChannel) Files.newByteChannel(filea);
				FileChannel chanb = (FileChannel) Files.newByteChannel(fileb)) {
			// checking if every portion of the two files are equals
			for (long position = 0; position < size; position += mapspan) {
				MappedByteBuffer mba = mapChannel(chana, position, size, mapspan);
				MappedByteBuffer mbb = mapChannel(chanb, position, size, mapspan);

				if (mba.compareTo(mbb) != 0) {
					return false;
				}

			}

		}
		return true;
	}

	// setting the value of variables to compare
	private static MappedByteBuffer mapChannel(FileChannel channel, long position, long size, int mapspan)
			throws IOException {
		final long end = Math.min(size, position + mapspan);
		final long maplen = (int) (end - position);
		return channel.map(MapMode.READ_ONLY, position, maplen);
	}

	public static void main(String[] args) throws Exception {

		// deleting previous output file and create a new output folder
		FileUtils.deleteDirectory(new File("output"));
		File indexOutput = new File("output");
		indexOutput.mkdir();
		
		// deleting previous mergedOutput file and create a new mergedOutput folder
		FileUtils.deleteDirectory(new File("mergedOutput"));
		File indexMerged = new File("mergedOutput");
		indexMerged.mkdir();

		
		int res = ToolRunner.run(new Configuration(), new Driver(), args);

		System.exit(res);

	}

}