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
 * �ڶ���MapReduce�������һ��MapReduce�õ������л��ļ������ÿ����Ĵ�������<class,TotalWords>
 * ���룺args[0],�����ʽΪ<<class:word>,counts>
 * �����args[1],�����ʽΪ<class,TotalWords>
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
		
		//������������ļ��ĸ�ʽΪ˳���ļ�
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapperClass(ClassTotalWordsMap.class); //����ִ��Map����
		job.setMapOutputKeyClass(Text.class);         //map�׶ε�key���������
		job.setMapOutputValueClass(IntWritable.class);//map�׶ε�value���������
		
		job.setReducerClass(ClassTotalWordsReduce.class);//����ִ��Reduce����
		job.setOutputKeyClass(Text.class);               //reduce�׶ε�key���������
		job.setOutputValueClass(IntWritable.class);      //reduce�׶ε�value��������� 
		
		//����job����������ļ�·��arg[0]��arg[1]
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
