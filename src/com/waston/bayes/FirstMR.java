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

/*
 * 第一个MapReduce用于处理训练集序列化后的文件，得到<<类名:单词>,单词出现次数>,即<<Class:word>,TotalCounts>
 * 输入:args[0],序列化的训练集,key为<类名:文档名>,value为文档中对应的单词.形式为<<ClassName:Doc>,word1 word2...>
 * 输出:args[1],key为<类名:单词>,value为单词出现次数,即<<Class:word>,TotalCounts>
 */
public class FirstMR {
	public static class ClassWordCountsMap extends Mapper<Text, Text, Text, IntWritable>{
		private Text mykey = new Text();
		private final IntWritable one = new IntWritable(1);
		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] line = key.toString().split(":");//line[0]:类名  line[1]:文档名
			String Class = line[0].toString();
			String[] words = value.toString().split(" ");//单词之间是以空格隔开的
			for(String word:words) {
				mykey.set(Class+":"+word);
				context.write(mykey,one);
			}
		}
		
	}
	
	public static class ClassWordCountsReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable value:values) {
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"FirstMR");
		job.setJarByClass(FirstMR.class);
		//设置输入输出文件的格式为顺序文件
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapperClass(ClassWordCountsMap.class); //设置执行Map的类
		job.setMapOutputKeyClass(Text.class);         //map阶段的key的输出类型
		job.setMapOutputValueClass(IntWritable.class);//map阶段的value的输出类型
		
		job.setReducerClass(ClassWordCountsReduce.class);//设置执行Reduce的类
		job.setOutputKeyClass(Text.class);               //reduce阶段的key的输出类型
		job.setOutputValueClass(IntWritable.class);      //reduce阶段的value的输出类型 
		
		//设置job的输入输出文件路径arg[0]、arg[1]
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
