package com.waston.bayes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


/**
 * 第三个mapreduce处理第一个mr程序的结果，得到训练集中的每个单词<word,one>
 * 输入:args[0],输入格式为<<class:word>,counts>
 * 输出:args[1],输出key为不重复单词,value为1.格式为<word,one>
 * @author zhang
 *
 */
public class ThirdMR {
	public static class WordOneMap extends Mapper<Text, IntWritable, Text, IntWritable>{
		private Text myKey = new Text();
		@Override
		protected void map(Text key, IntWritable value, Context context)
				throws IOException, InterruptedException {
			String[] line = key.toString().split(":");
			myKey.set(line[1]);
			context.write(myKey, value);
		}
	}
	
	public static class WordOneReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		private final IntWritable one = new IntWritable(1);
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			context.write(key, one);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"ThirdMR");
		job.setJarByClass(ThirdMR.class);
		
		//设置输入输出文件的格式为顺序文件
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapperClass(WordOneMap.class); //设置执行Map的类
		job.setMapOutputKeyClass(Text.class);         //map阶段的key的输出类型
		job.setMapOutputValueClass(IntWritable.class);//map阶段的value的输出类型
		
		job.setReducerClass(WordOneReduce.class);//设置执行Reduce的类
		job.setOutputKeyClass(Text.class);               //reduce阶段的key的输出类型
		job.setOutputValueClass(IntWritable.class);      //reduce阶段的value的输出类型 
		
		//设置job的输入输出文件路径arg[0]、arg[1]
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
