package com.waston.bayes;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * �����mr�������bayes����
 * ����:args[4]:���Լ������л��ĵ�Ŀ¼      keyΪ<����:�ĵ���>,valueΪ�ĵ��ж�Ӧ�ĵ���.��ʽΪ<<ClassName:Doc>,word1 word2...>
 *     �������HashMap<String,Double> priorProbably
 *     �������HashMap<String,Double> conditionProbably
 * ���:args[5]:�����������л��ĵ�Ŀ¼  ��ʽΪ��<�ĵ���,�ĵ�������>����<doc,class> 
 * @author zhang
 *
 */
public class FifthMR {
	private static HashMap<String,Double> priorProbably = new HashMap<String,Double>();
	private static HashMap<String,Double> conditionProbably = new HashMap<String,Double>();
	/**
	 * ������������P(t|c)=(����t�����c���ĵ��г��ֵĴ��� + 1)/(c�����Ĵ�������+ѵ�����Ĵ�������)
	 * ��Ҫ��ǰ����mr��������л������Ϊ����args[0] args[1] args[2]
	 * ����:��Ӧ��һ��MapReduce�����<<class,word>,counts>,�ڶ���MapReduce�����<class,totalWords>,
	 *     ������MapReduce�����<word,one>
	 * ���:��HashMap<String,Double>��ŵ�<<����:����>,��������>
	 * @return
	 * @throws IOException 
	 */
	public static HashMap<String,Double> GetConditionProbably() throws IOException{
		Configuration conf = new Configuration();
		//���ÿ����Ĵ����ܸ���,<Class,TotalWords>
		HashMap<String, Double> ClassTotalWords = new HashMap<String, Double>();
//		String ClassWordCountsPath = otherArgs[0]+"/part-r-00000";//��1��mr�Ľ��·��
//		String ClassTotalWordsPath = otherArgs[1]+"/part-r-00000";//��2��mr�Ľ��·��
//		String TotalTermsPath = otherArgs[2]+"/part-r-00000";//��3��mr�Ľ��·��
		
		String ClassWordCountsPath = "/M/FirstMR"+"/part-r-00000";//��1��mr�Ľ��·��
		String ClassTotalWordsPath = "/M/SecondMR"+"/part-r-00000";//��2��mr�Ľ��·��
		String TotalTermsPath = "/M/ThirdMR"+"/part-r-00000";//��3��mr�Ľ��·��
		
		double TotalTerms = 0.0;
		
		FileSystem fs1 = FileSystem.get(URI.create(ClassTotalWordsPath), conf);
		Path path1 = new Path(ClassTotalWordsPath);
		SequenceFile.Reader reader1 = null;
		//��<class,totalWords>��ȡ����ClassTotalWords��
		try{
			reader1 = new SequenceFile.Reader(fs1, path1, conf);
			Text key1 = (Text)ReflectionUtils.newInstance(reader1.getKeyClass(), conf);
			IntWritable value1 = (IntWritable)ReflectionUtils.newInstance(reader1.getValueClass(), conf);
			while(reader1.next(key1,value1)){
				ClassTotalWords.put(key1.toString(), value1.get()*1.0);
			}
		}finally{
			IOUtils.closeStream(reader1);
		}
		
		//���ѵ���������еĲ��ظ����ʵ�����������������
		FileSystem fs2 = FileSystem.get(URI.create(TotalTermsPath), conf);
		Path path2 = new Path(TotalTermsPath);
		SequenceFile.Reader reader2 = null;
		try{
			reader2 = new SequenceFile.Reader(fs2, path2, conf);
			Text key2 = (Text)ReflectionUtils.newInstance(reader2.getKeyClass(), conf);
			IntWritable value2 = (IntWritable)ReflectionUtils.newInstance(reader2.getValueClass(), conf);
			while(reader2.next(key2,value2)){
				TotalTerms += value2.get();
			}	
			System.out.println(TotalTerms);
		}finally{
			IOUtils.closeStream(reader2);
		}
		
		//����<<class,word>,counts>,���ÿ��������һ������µ���������
		FileSystem fs3 = FileSystem.get(URI.create(ClassWordCountsPath), conf);
		Path path3 = new Path(ClassWordCountsPath);
		SequenceFile.Reader reader3 = null;
		try{
			reader3 = new SequenceFile.Reader(fs3, path3, conf);
			Text key3 = (Text)ReflectionUtils.newInstance(reader3.getKeyClass(), conf);
			IntWritable value3 = (IntWritable)ReflectionUtils.newInstance(reader3.getValueClass(), conf);
			Text newKey = new Text();
			while(reader3.next(key3,value3)){
				String[] line = key3.toString().split(":");
				newKey.set(line[0]);//�õ��������ڵ���
				conditionProbably.put(key3.toString(), (value3.get()+1)/(ClassTotalWords.get(newKey.toString())+TotalTerms));
			}
			//����ͬһ�����û�г��ֹ��ĵ��ʵĸ���һ����1/(ClassTotalWords.get(class) + TotalTerms)
			//�����࣬ÿ��������ټ�һ��û�г��ֵ��ʵĸ��ʣ����ʽΪ<class,probably>
			for(Map.Entry<String,Double> entry:ClassTotalWords.entrySet()){
				conditionProbably.put(entry.getKey().toString(), 1.0/(ClassTotalWords.get(entry.getKey().toString()) + TotalTerms));
			}
		}finally{
			IOUtils.closeStream(reader3);
		}
		return conditionProbably;
	}
	
	/**
	 * �����������P(c)=��c�µ��ļ�����/ѵ�����µ��ļ�����
	 * ����:��4��mr��������л��ļ�<class,docnum> args[3]
	 * ���:��HashMap<String,Double>��ŵ�<����,�������>
	 * @throws IOException 
	 *
	 */
	public static HashMap<String, Double> GetPriorProbably() throws IOException{
		Configuration conf = new Configuration();
		String filePath = "/M/FourthMR"+"/part-r-00000";
		FileSystem fs = FileSystem.get(URI.create(filePath), conf);
		Path path = new Path(filePath);
		SequenceFile.Reader reader = null;
		double totalDocs = 0;
		try{
			reader = new SequenceFile.Reader(fs, path, conf);
			Text key = (Text)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			IntWritable value = (IntWritable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
			long position = reader.getPosition();//���ñ�ǵ㣬����ĵ���ʼλ��
			while(reader.next(key,value)){
				totalDocs += value.get();//�õ�ѵ�������ĵ���
			}
			
			reader.seek(position);//���õ�ǰ�涨λ�ı�ǵ�
			while(reader.next(key,value)){
				priorProbably.put(key.toString(), value.get()/totalDocs);
			}
		}finally{
			IOUtils.closeStream(reader);
		}
		return priorProbably;
	}
	
	public static class PredictMap extends Mapper<Text, Text, Text, Text>{
		@Override
		protected void setup(Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			GetPriorProbably();
			GetConditionProbably();
		}
		
		private Text myKey = new Text();
		private Text myValue = new Text();
		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] line = key.toString().split(":");
			myKey.set(line[1]);//keyΪ�ĵ���<docID>
			//���������࣬������ĵ���ÿһ�����еĸ��ʣ�map�������ʽΪ<docID,<��,����>>
			for(Map.Entry<String, Double> entry:priorProbably.entrySet()) {
				String className = entry.getKey();//�õ�����  
				double  tmpValue = Math.log(entry.getValue());//��ά����ֹ��˸��������
				String[] words = value.toString().split(" ");
				for(String word:words) {//�ڲ��������ÿ�����ʵ����ڵ�ǰ��ĸ��ʽ���log���
					String tmpKey = className + ":" + word;
					//ʹ��conditionProbably�����������ʣ�log���
					if(conditionProbably.containsKey(tmpKey)) {
						tmpValue += Math.log(conditionProbably.get(tmpKey));
					}else {
						//�����Լ��ĵ�����ѵ����δ���֣������֮ǰ��������������ĸ���
					    tmpValue += Math.log(conditionProbably.get(className));
					}
					
				}
				myValue.set(className + ":" + tmpValue);
				context.write(myKey, myValue);
				
			}
			
		}
	}
	
	public static class PredictReduce extends Reducer<Text, Text, Text, Text>{
		private Text myValue = new Text();
		//reduce�׶��յ��ĸ�ʽΪ��<<docID>,<��1:����1> <��2:����2>...>
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			double maxProbably = 0.0;
			boolean flag = true;
			String tmpClass = null;
			for(Text value:values) {
				String[] line = value.toString().split(":");
				if(flag) {//�����ǵ�һ�ν���ѭ��
					tmpClass = line[0];
					maxProbably = Double.parseDouble(line[1]);
					flag = false;
				}else {
					if(maxProbably < Double.parseDouble(line[1])) {
						//�����ʱ�������maxProbably
						tmpClass = line[0];
						maxProbably = Double.parseDouble(line[1]);
					}
				}
			}
			myValue.set(tmpClass);
			context.write(key, myValue);
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		//otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(args.length != 6) {
			System.err.println("Wrong Usage!");
			System.exit(0);
		}
		Job job = Job.getInstance(conf,"FifthMR");
		job.setJarByClass(FifthMR.class);
		
		
		//������������ļ��ĸ�ʽΪ˳���ļ�
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapperClass(PredictMap.class); //����ִ��Map����
		job.setMapOutputKeyClass(Text.class);         //map�׶ε�key���������
		job.setMapOutputValueClass(Text.class);//map�׶ε�value���������
		
		job.setReducerClass(PredictReduce.class);//����ִ��Reduce����
		job.setOutputKeyClass(Text.class);               //reduce�׶ε�key���������
		job.setOutputValueClass(Text.class);      //reduce�׶ε�value��������� 
		
		//����job����������ļ�·��arg[0]��arg[1]
		FileInputFormat.addInputPath(job, new Path("/M/testset"));
		FileOutputFormat.setOutputPath(job, new Path("/M/result"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
