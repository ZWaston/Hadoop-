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
 * 第五个mr程序进行bayes测试
 * 输入:args[4]:测试集的序列化文档目录      key为<类名:文档名>,value为文档中对应的单词.形式为<<ClassName:Doc>,word1 word2...>
 *     先验概率HashMap<String,Double> priorProbably
 *     后验概率HashMap<String,Double> conditionProbably
 * 输出:args[5]:分类结果的序列化文档目录  格式为：<文档名,文档所属类>，即<doc,class> 
 * @author zhang
 *
 */
public class FifthMR {
	private static HashMap<String,Double> priorProbably = new HashMap<String,Double>();
	private static HashMap<String,Double> conditionProbably = new HashMap<String,Double>();
	/**
	 * 计算条件概率P(t|c)=(单词t在类别c的文档中出现的次数 + 1)/(c类类别的词条总数+训练集的词项总数)
	 * 需要用前三个mr程序的序列化结果作为输入args[0] args[1] args[2]
	 * 输入:对应第一个MapReduce的输出<<class,word>,counts>,第二个MapReduce的输出<class,totalWords>,
	 *     第三个MapReduce的输出<word,one>
	 * 输出:用HashMap<String,Double>存放的<<类名:单词>,条件概率>
	 * @return
	 * @throws IOException 
	 */
	public static HashMap<String,Double> GetConditionProbably() throws IOException{
		Configuration conf = new Configuration();
		//存放每个类的词条总个数,<Class,TotalWords>
		HashMap<String, Double> ClassTotalWords = new HashMap<String, Double>();
//		String ClassWordCountsPath = otherArgs[0]+"/part-r-00000";//第1个mr的结果路径
//		String ClassTotalWordsPath = otherArgs[1]+"/part-r-00000";//第2个mr的结果路径
//		String TotalTermsPath = otherArgs[2]+"/part-r-00000";//第3个mr的结果路径
		
		String ClassWordCountsPath = "/M/FirstMR"+"/part-r-00000";//第1个mr的结果路径
		String ClassTotalWordsPath = "/M/SecondMR"+"/part-r-00000";//第2个mr的结果路径
		String TotalTermsPath = "/M/ThirdMR"+"/part-r-00000";//第3个mr的结果路径
		
		double TotalTerms = 0.0;
		
		FileSystem fs1 = FileSystem.get(URI.create(ClassTotalWordsPath), conf);
		Path path1 = new Path(ClassTotalWordsPath);
		SequenceFile.Reader reader1 = null;
		//将<class,totalWords>读取存入ClassTotalWords中
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
		
		//算出训练集中所有的不重复单词的数量，即词项总数
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
		
		//遍历<<class,word>,counts>,算出每个单词在一个类别下的条件概率
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
				newKey.set(line[0]);//得到单词所在的类
				conditionProbably.put(key3.toString(), (value3.get()+1)/(ClassTotalWords.get(newKey.toString())+TotalTerms));
			}
			//对于同一个类别没有出现过的单词的概率一样，1/(ClassTotalWords.get(class) + TotalTerms)
			//遍历类，每个类别中再加一个没有出现单词的概率，其格式为<class,probably>
			for(Map.Entry<String,Double> entry:ClassTotalWords.entrySet()){
				conditionProbably.put(entry.getKey().toString(), 1.0/(ClassTotalWords.get(entry.getKey().toString()) + TotalTerms));
			}
		}finally{
			IOUtils.closeStream(reader3);
		}
		return conditionProbably;
	}
	
	/**
	 * 计算先验概率P(c)=类c下的文件总数/训练集下的文件总数
	 * 输入:第4个mr程序的序列化文件<class,docnum> args[3]
	 * 输出:用HashMap<String,Double>存放的<类名,先验概率>
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
			long position = reader.getPosition();//设置标记点，标记文档起始位置
			while(reader.next(key,value)){
				totalDocs += value.get();//得到训练集总文档数
			}
			
			reader.seek(position);//重置到前面定位的标记点
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
			myKey.set(line[1]);//key为文档名<docID>
			//遍历所有类，算出该文档在每一个类中的概率，map的输出格式为<docID,<类,概率>>
			for(Map.Entry<String, Double> entry:priorProbably.entrySet()) {
				String className = entry.getKey();//得到类名  
				double  tmpValue = Math.log(entry.getValue());//降维，防止相乘浮点数溢出
				String[] words = value.toString().split(" ");
				for(String word:words) {//内层遍历，对每个单词的属于当前类的概率进行log相加
					String tmpKey = className + ":" + word;
					//使用conditionProbably查找条件概率，log相加
					if(conditionProbably.containsKey(tmpKey)) {
						tmpValue += Math.log(conditionProbably.get(tmpKey));
					}else {
						//若测试集的单词在训练集未出现，则加上之前在条件概率中算的概率
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
		//reduce阶段收到的格式为：<<docID>,<类1:概率1> <类2:概率2>...>
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			double maxProbably = 0.0;
			boolean flag = true;
			String tmpClass = null;
			for(Text value:values) {
				String[] line = value.toString().split(":");
				if(flag) {//表明是第一次进入循环
					tmpClass = line[0];
					maxProbably = Double.parseDouble(line[1]);
					flag = false;
				}else {
					if(maxProbably < Double.parseDouble(line[1])) {
						//若概率变大，则更新maxProbably
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
		
		
		//设置输入输出文件的格式为顺序文件
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapperClass(PredictMap.class); //设置执行Map的类
		job.setMapOutputKeyClass(Text.class);         //map阶段的key的输出类型
		job.setMapOutputValueClass(Text.class);//map阶段的value的输出类型
		
		job.setReducerClass(PredictReduce.class);//设置执行Reduce的类
		job.setOutputKeyClass(Text.class);               //reduce阶段的key的输出类型
		job.setOutputValueClass(Text.class);      //reduce阶段的value的输出类型 
		
		//设置job的输入输出文件路径arg[0]、arg[1]
		FileInputFormat.addInputPath(job, new Path("/M/testset"));
		FileOutputFormat.setOutputPath(job, new Path("/M/result"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
