package com.waston.bayes;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/*
 * args[0]����,���Լ�·��
 * args[1]���,������Լ��õ��õ���ȷ�����ÿ��������Щ�ĵ�
 * args[2]����,����Ҷ˹����Ľ��
 * args[3]���,����Ҷ˹����ÿ��������Щ�ĵ�
 * ������,�������õ�����ֵ(ֱ����ʾ���ն�)]
 */
public class Evaluation {
	/**
	 * �õ�ԭ�����ĵ�����
	 * ����:��ʼ���ݼ���,��ʽΪ<<ClassName:Doc>,word1 word2...>
	 * ���:ԭ�����ĵ����࣬��<ClassName,Doc>
	 */
	public static class OriginalDocOfClassMap extends Mapper<Text, Text, Text, Text>{
		private Text newKey = new Text();
		private Text newValue = new Text();
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException{	
			String[] line = key.toString().split(":");
			newKey.set(line[0]);
			newValue.set(line[1]);
			context.write(newKey, newValue);
		}
	}
	
	/**
	 * �õ�����Ҷ˹�ַ������������ĵ�����
	 * ��ȡ����Ҷ˹�����������Ľ���ĵ�<Doc,ClassName>,������ת��Ϊ<ClassName,Doc>��ʽ
	 */
	public static class ClassifiedDocOfClassMap extends Mapper<Text, Text, Text, Text>{		
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException{		
			context.write(value, key);
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		private Text result = new Text();		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			//�����ĵ��б�
			String fileList = new String();
			for(Text value:values){
				fileList += value.toString() + ";";
			}
			result.set(fileList);
			context.write(key, result);
		}
	}
	
	/**
	 * ��һ��MapReduce����ó���ʼ����¸���������Щ�ĵ�,�ڶ���MapReduce����ó�����Ҷ˹��������������Щ�ĵ�
	 * �˺������þ���ͳ�Ƴ�ʼ����µķ���ͱ�Ҷ˹������������¸����๫�е��ĵ���Ŀ(����Ը����������ȷ���ĵ���ĿTP)
	 * ��ʼ����µĸ���������Ŀ��ȥ������ȷ����Ŀ��Ϊԭ����ȷ������������Ŀ(FN = OriginalCounts-TP)
	 * ��Ҷ˹����õ��ĸ����������Ŀ��ȥ������ȷ����Ŀ��Ϊԭ�������ڸ��൫�ֵ��������Ŀ(FP = ClassifiedCounts - TP)
	 */
	//Precision����:P = TP/(TP+FP)
	//Recall����:   R = TP/(TP+FN)
	//P��R�ĵ���ƽ��:F1 = 2PR/(P+R)
	//����������:  
	//Macroaveraged precision:(p1+p2+...+pN)/N
	//Microaveraged precision:��Ӧ��������ټ����ܵ�P��Rֵ	
	public static void GetEvaluation(Configuration conf, String ClassifiedDocOfClassFilePath, String OriginalDocOfClassFilePath) throws IOException{
		FileSystem fs1 = FileSystem.get(URI.create(ClassifiedDocOfClassFilePath), conf);
		Path path1 = new Path(ClassifiedDocOfClassFilePath);
		SequenceFile.Reader reader1 = null;
		
		FileSystem fs2 = FileSystem.get(URI.create(OriginalDocOfClassFilePath), conf);
		Path path2 = new Path(OriginalDocOfClassFilePath);
		SequenceFile.Reader reader2 = null;
		try{
			reader1 = new SequenceFile.Reader(fs1, path1, conf);//����Reader����			
			Text key1 = (Text)ReflectionUtils.newInstance(reader1.getKeyClass(), conf);
			Text value1 = (Text)ReflectionUtils.newInstance(reader1.getValueClass(), conf);
			
			reader2 = new SequenceFile.Reader(fs2, path2, conf);
			Text key2 = (Text)ReflectionUtils.newInstance(reader2.getKeyClass(), conf);
			Text value2 = (Text)ReflectionUtils.newInstance(reader2.getValueClass(), conf);
			
			ArrayList<String> ClassNames = new ArrayList<String>();     //���εõ����������
			ArrayList<Integer> TruePositive = new ArrayList<Integer>(); //��¼��ʵ����;��������ȷ������ĵ���Ŀ
			ArrayList<Integer> FalseNegative = new ArrayList<Integer>();//��¼���ڸ��൫��û�зֵ��������Ŀ
			ArrayList<Integer> FalsePositive = new ArrayList<Integer>();//��¼�����ڸ��൫�Ǳ��ֵ��������Ŀ
			
		
			HashMap<String,String> originalMap = new HashMap<String,String>();
			HashMap<String,String> classifiedMap = new HashMap<String,String>();
			while(reader1.next(key1, value1)) {
				classifiedMap.put(key1.toString(), value1.toString());
			}
			while(reader2.next(key2, value2)) {
				originalMap.put(key2.toString(), value2.toString());
			}
			//����originalMap��ÿһ���࣬����classifiedMap��û�е��࣬TP��Ϊ0,FPΪ0
			for(Map.Entry<String, String> originalEntry:originalMap.entrySet()) {
				ClassNames.add(originalEntry.getKey().toString());
				String[] originalValues = originalEntry.getValue().toString().split(";");
				String originalKey = originalEntry.getKey().toString();
				String[] classifiedValues = null;
				int TP = 0;
				double pp = 0.0;
				double rr = 0.0;
				double ff = 0.0;
				if(classifiedMap.get(originalKey)==null) {
					//��ԭ���Լ���һ���࣬������NBģ�͵õ��ķ���ȴû������࣬��ʱtp fp Ϊ0
					TP = 0;
					TruePositive.add(TP);
					FalsePositive.add(0);
					FalseNegative.add(originalValues.length - TP);
					pp = 0.5;
					rr = TP*1.0/originalValues.length;
					ff = 0.0;
					int zero = 0;
				    
					Formatter f = new Formatter(System.out);
					f.format("%-10s: %-4d %-4d %-4d %-4d %-4d p=%.16f r=%.16f f1=%.16f\n", 
							originalEntry.getKey().toString(),originalValues.length,zero,
					     TP,(originalValues.length-TP),zero,pp,rr,ff);
					
				}else {
					
					classifiedValues = classifiedMap.get(originalKey).toString().split(";");
					for(String str1:originalValues){
						for(String str2:classifiedValues){
							if(str1.equals(str2)){
								TP++;
							}
						}
					}
					
					TruePositive.add(TP);
					FalsePositive.add(classifiedValues.length - TP);
					FalseNegative.add(originalValues.length - TP);	
					
					pp = TP*1.0/classifiedValues.length;
					rr = TP*1.0/originalValues.length;
					ff = 2*pp*rr/(pp+rr);
					
					Formatter f = new Formatter(System.out);
					f.format("%-10s: %-4d %-4d %-4d %-4d %-4d p=%.16f r=%.16f f1=%.16f\n", 
							originalEntry.getKey().toString(),originalValues.length,classifiedValues.length,
					     TP,(originalValues.length-TP),(classifiedValues.length-TP),pp,rr,ff);
				}
				
			}
			
			
			//Caculate MacroAverage
			double Pprecision = 0.0;
			double Rprecision = 0.0;
			double F1precision = 0.0;

			//Calculate MicroAverage
			int TotalTP = 0;
			int TotalFN = 0;
			int TotalFP = 0;			
			
			for(int i=0; i<ClassNames.size(); i++){			
				//MacroAverage	
				double p1 = 0.0;
				if(TruePositive.get(i) == 0) {
					p1 = 0.5;
				}else {
					p1 = TruePositive.get(i)*1.0/(TruePositive.get(i) + FalsePositive.get(i));
				}
				double r1 = TruePositive.get(i)*1.0/(TruePositive.get(i) + FalseNegative.get(i));
				double f1 = 2.0*p1*r1/(p1+r1);
				//System.out.println(ClassNames.get(i)+": p1="+p1+";\tr1="+r1+"\tf1="+f1);
				Pprecision += p1;
				Rprecision += r1;
				F1precision += f1;
								
				//MicroAverage
				TotalTP += TruePositive.get(i);
				TotalFN += FalseNegative.get(i);
				TotalFP += FalsePositive.get(i);
			}
			System.out.println("MacroAverage precision : P= " + Pprecision/ClassNames.size() +";\tR="+ Rprecision/ClassNames.size() +";\tF1="+F1precision/ClassNames.size());			
			double p2 = TotalTP*1.0/(TotalTP + TotalFP);
			double r2 = TotalTP*1.0/(TotalTP + TotalFN);
			double f2 = 2.0*p2*r2/(p2+r2);
			
			System.out.println("MicroAverage precision : P= " + p2 + ";\tR=" + r2 + ";\tF1=" + f2);
			
		}finally{
			reader1.close();
			reader2.close();
		}		
	}
	
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		String[] otherArgs = args;
		if (otherArgs.length != 4) {
			System.err.println("Usage: NaiveBayesClassification!");
			System.exit(4);
		}		
		FileSystem hdfs = FileSystem.get(conf);
		
		Path path1 = new Path(otherArgs[1]);
		if(hdfs.exists(path1))
			hdfs.delete(path1, true);
		Job job1 = new Job(conf, "Original");
		job1.setJarByClass(Evaluation.class);
		job1.setInputFormatClass(SequenceFileInputFormat.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		job1.setMapperClass(OriginalDocOfClassMap.class);
		//job1.setCombinerClass(Reduce.class);
		job1.setReducerClass(Reduce.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		//����������� 
		ControlledJob ctrljob1 = new  ControlledJob(conf);
		ctrljob1.setJob(job1);
		//job1����������ļ�·��
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
		
		Path path2 = new Path(otherArgs[3]);
		if(hdfs.exists(path2))
			hdfs.delete(path2, true);
		Job job2 = new Job(conf, "Classified");
		job2.setJarByClass(Evaluation.class);	
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputFormatClass(SequenceFileOutputFormat.class);
		job2.setMapperClass(ClassifiedDocOfClassMap.class);
		job2.setReducerClass(Reduce.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		//����������� 
		ControlledJob ctrljob2 = new  ControlledJob(conf);
		ctrljob2.setJob(job2);
		//job1����������ļ�·��
		FileInputFormat.addInputPath(job2, new Path(otherArgs[2]+"/part-r-00000"));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));
		
		ctrljob2.addDependingJob(ctrljob1);
		
		JobControl jobCtrl = new JobControl("NaiveBayes");
		//��ӵ��ܵ�JobControl����п���
		jobCtrl.addJob(ctrljob1);
		jobCtrl.addJob(ctrljob2);
		
		//���߳���������סһ��Ҫ�����
	    Thread  theController = new Thread(jobCtrl); 
	    theController.start(); 
	    while(true){
	        if(jobCtrl.allFinished()){//�����ҵ�ɹ���ɣ��ʹ�ӡ�ɹ���ҵ����Ϣ 
	        	System.out.println(jobCtrl.getSuccessfulJobList()); 
	        	jobCtrl.stop(); 
	        	break; 
	        }
	    }	
	    
	    GetEvaluation(conf, otherArgs[3]+"/part-r-00000", otherArgs[1]+"/part-r-00000");
	}

}
