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
 * 第二个MapReduce程序处理第一个MapReduce得到的序列化文件，算出每个类的词条总数<class,TotalWords>
 * 输入：args[0],输入格式为<<class:word>,counts>
 * 输出：args[1],输出格式为<class,TotalWords>
 * @author zhang
 *
 */
public class SecondMR {
	public static class ClassTotalWordsMap extends Mapper<Text, IntWritable, Text, IntWritable>{
		private Text myKey = new Text();
		@Override
		protected void map(Text key, IntWritable value,Context context)
				throws IOException, InterruptedException {
			String[] line = key.toString().split(":");
			myKey.set(line[0]);
			context.write(myKey, value);
		}
	}
	
	public static class ClassTotalWordsReduce extends Reducer<Text, IntWritable,Text, IntWritable>{
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
		Job job = Job.getInstance(conf,"SecondMR");
		job.setJarByClass(SecondMR.class);
		
		//设置输入输出文件的格式为顺序文件
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapperClass(ClassTotalWordsMap.class); //设置执行Map的类
		job.setMapOutputKeyClass(Text.class);         //map阶段的key的输出类型
		job.setMapOutputValueClass(IntWritable.class);//map阶段的value的输出类型
		
		job.setReducerClass(ClassTotalWordsReduce.class);//设置执行Reduce的类
		job.setOutputKeyClass(Text.class);               //reduce阶段的key的输出类型
		job.setOutputValueClass(IntWritable.class);      //reduce阶段的value的输出类型 
		
		//设置job的输入输出文件路径arg[0]、arg[1]
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
