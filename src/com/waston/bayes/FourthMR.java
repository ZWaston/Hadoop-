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
 * ���ĸ�mapreduce����ѵ�������л��ļ����õ�<class,docnum>,��<��,�����µ��ĵ�����>
 * ���mr�������ҪĿ�������ĵ����������
 * ����:args[0],���л���ѵ����,keyΪ<����:�ĵ���>,valueΪ�ĵ��ж�Ӧ�ĵ���.��ʽΪ<<ClassName:Doc>,word1 word2...>
 * ���:args[1],keyΪ����,valueΪ���Ӧ���ĵ���Ŀ,��<class,docnum> 
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
		
		//������������ļ��ĸ�ʽΪ˳���ļ�
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapperClass(ClassDocnumMap.class); //����ִ��Map����
		job.setMapOutputKeyClass(Text.class);         //map�׶ε�key���������
		job.setMapOutputValueClass(IntWritable.class);//map�׶ε�value���������
		
		job.setReducerClass(ClassDocnumReduce.class);//����ִ��Reduce����
		job.setOutputKeyClass(Text.class);               //reduce�׶ε�key���������
		job.setOutputValueClass(IntWritable.class);      //reduce�׶ε�value��������� 
		
		//����job����������ļ�·��arg[0]��arg[1]
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}