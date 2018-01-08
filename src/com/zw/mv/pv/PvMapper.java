package com.zw.mv.pv;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * KEYIN:��MR�ṩ�ĳ����ȡ����һ�е���ʼƫ����   Long
 * VALUEIN:��MR�ṩ�ĳ����ȡ����һ�е����ݵ����� String
 * 
 * KEYOUT�����û����߼��������������֮�󷵻ص�key������
 * VALUEOUT�����û����߼��������������֮�󷵻ص�value������
 * 
 * 
 * Long  String Integer�����Ͳ�����hadoop��ʹ��
 * 
 * �ֱ��ӦLongWritable��Text��IntWritable
 *
 */
public class PvMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    /**
     * MR����ÿ��һ�о͵���һ�����map����
     */
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }

}
