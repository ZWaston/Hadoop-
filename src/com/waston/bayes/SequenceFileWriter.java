package com.waston.bayes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
/**
 * 该类的作用主要是把NBCorpus文件夹的许多小文件序列化成一个文件
 * 文件存放的格式为：<<所属类别:文档名>,文档内容>
 * @author zhang
 *
 */
public class SequenceFileWriter {
	
	/**
	 * 该函数作用读取文件内容,一个参数:
	 * 输入：文件名(file)，文件的格式是一行一个单词
	 * 输出：文件内容合并成的字符串(result)
	 */	
	private static String fileToString(File file) throws IOException{
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line = null;
		String result = "";
		while((line = reader.readLine()) != null){
			if(line.matches("[a-zA-Z]+")){ //只保留文件中字母单词，去除数字
				result += line + " ";      //单词之间以空格分开
			}
		}
		
		reader.close();
		return result;
	}
	
	/**
	 * 将一个文件夹下的所有文件序列化,两个参数:
     * 输入：args[0]：准备序列化的文件的路径，Path
     * 输出：args[1]：序列化后准备输出的文件路径名字，URI
     * eg:arg[0]="/home/zhang/NBCorpus/Country" arg[1]="hdfs://192.168.74.130:9000/Country_data"
     */
	public static void main(String[] args) throws IOException {
		File[] dirs = new File(args[0]).listFiles();
		
		String uri = args[1];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		
		Text key = new Text();
		Text value = new Text();
		
		SequenceFile.Writer writer = null;
		try{
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
		
			for(File dir:dirs){
				File[] files = dir.listFiles();
				for(File file:files){
					//key：目录名+":"+文件名					
					key.set(dir.getName() + ":" + file.getName());
					//value：文件内容
					value.set(fileToString(file));
					writer.append(key, value);
					//System.out.println(key + "\t" + value);
				}
			}
		}finally{
			IOUtils.closeStream(writer);
		}		
	}

}
