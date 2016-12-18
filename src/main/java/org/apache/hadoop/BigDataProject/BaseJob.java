package org.apache.hadoop.BigDataProject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;


public abstract class BaseJob extends Configured implements Tool {

	// method to set the configuration for the job and the mapper and the reducer classes
	protected Job setupJob(String jobName,JobInfo jobInfo) throws Exception {
		
		
		Job job = Job.getInstance(new Configuration());

		// set the several classes
		job.setJarByClass(jobInfo.getJarByClass());

		//set the mapper class
		job.setMapperClass(jobInfo.getMapperClass());

		//set the reducer class
		job.setReducerClass(jobInfo.getReducerClass());
		
		//set the number of the reducers
		job.setNumReduceTasks(3);
	
		// set the type of the output key and value for the Map & Reduce functions	
		job.setOutputKeyClass(jobInfo.getOutputKeyClass());
		job.setOutputValueClass(jobInfo.getOutputValueClass());
		
		return job;
	}
	
	protected abstract class JobInfo {
		public abstract Class<?> getJarByClass();
		public abstract Class<? extends Mapper> getMapperClass();
		public abstract Class<? extends Reducer> getReducerClass();
		public abstract Class<?> getOutputKeyClass();
		public abstract Class<?> getOutputValueClass();
		
 	}
}