package com.zw.mv.pv;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PvJobRun {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(PvJobRun.class);
		
		job.setMapperClass(PvMapper.class);
		job.setReducerClass(PvReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    
	    job.setOutputFormatClass(TextOutputFormat.class);
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    boolean wfc = job.waitForCompletion(true);
	    
	    System.exit(wfc?0:1);
				
	}
}
