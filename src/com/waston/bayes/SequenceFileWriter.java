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
 * �����������Ҫ�ǰ�NBCorpus�ļ��е����С�ļ����л���һ���ļ�
 * �ļ���ŵĸ�ʽΪ��<<�������:�ĵ���>,�ĵ�����>
 * @author zhang
 *
 */
public class SequenceFileWriter {
	
	/**
	 * �ú������ö�ȡ�ļ�����,һ������:
	 * ���룺�ļ���(file)���ļ��ĸ�ʽ��һ��һ������
	 * ������ļ����ݺϲ��ɵ��ַ���(result)
	 */	
	private static String fileToString(File file) throws IOException{
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line = null;
		String result = "";
		while((line = reader.readLine()) != null){
			if(line.matches("[a-zA-Z]+")){ //ֻ�����ļ�����ĸ���ʣ�ȥ������
				result += line + " ";      //����֮���Կո�ֿ�
			}
		}
		
		reader.close();
		return result;
	}
	
	/**
	 * ��һ���ļ����µ������ļ����л�,��������:
     * ���룺args[0]��׼�����л����ļ���·����Path
     * �����args[1]�����л���׼��������ļ�·�����֣�URI
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
					//key��Ŀ¼��+":"+�ļ���					
					key.set(dir.getName() + ":" + file.getName());
					//value���ļ�����
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
