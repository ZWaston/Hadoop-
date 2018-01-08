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
 * 第四个mapreduce处理训练集序列化文件，得到<class,docnum>,即<类,该类下的文档总数>
 * 这个mr程序的主要目的是求文档的先验概率
 * 输入:args[0],序列化的训练集,key为<类名:文档名>,value为文档中对应的单词.形式为<<ClassName:Doc>,word1 word2...>
 * 输出:args[1],key为类名,value为类对应的文档数目,即<class,docnum> 
 * @author zhang
 *
 */
public class FourthMR {
	public static class ClassDocnumMap extends Mapper<Text, Text, Text, IntWritable>{
		private Text mykey = new Text();
		private final IntWritable one = new IntWritable(1);
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] line = key.toString().split(":");
			mykey.set(line[0]);
			context.write(mykey, one);
		}
	}
	
	public static class ClassDocnumReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
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
		Job job = Job.getInstance(conf,"FourthMR");
		job.setJarByClass(FourthMR.class);
		
		//设置输入输出文件的格式为顺序文件
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapperClass(ClassDocnumMap.class); //设置执行Map的类
		job.setMapOutputKeyClass(Text.class);         //map阶段的key的输出类型
		job.setMapOutputValueClass(IntWritable.class);//map阶段的value的输出类型
		
		job.setReducerClass(ClassDocnumReduce.class);//设置执行Reduce的类
		job.setOutputKeyClass(Text.class);               //reduce阶段的key的输出类型
		job.setOutputValueClass(IntWritable.class);      //reduce阶段的value的输出类型 
		
		//设置job的输入输出文件路径arg[0]、arg[1]
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
