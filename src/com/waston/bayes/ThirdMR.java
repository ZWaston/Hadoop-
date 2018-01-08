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
 * ������mapreduce�����һ��mr����Ľ�����õ�ѵ�����е�ÿ������<word,one>
 * ����:args[0],�����ʽΪ<<class:word>,counts>
 * ���:args[1],���keyΪ���ظ�����,valueΪ1.��ʽΪ<word,one>
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
		
		//������������ļ��ĸ�ʽΪ˳���ļ�
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapperClass(WordOneMap.class); //����ִ��Map����
		job.setMapOutputKeyClass(Text.class);         //map�׶ε�key���������
		job.setMapOutputValueClass(IntWritable.class);//map�׶ε�value���������
		
		job.setReducerClass(WordOneReduce.class);//����ִ��Reduce����
		job.setOutputKeyClass(Text.class);               //reduce�׶ε�key���������
		job.setOutputValueClass(IntWritable.class);      //reduce�׶ε�value��������� 
		
		//����job����������ļ�·��arg[0]��arg[1]
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
