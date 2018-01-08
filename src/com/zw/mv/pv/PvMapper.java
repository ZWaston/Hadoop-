package com.zw.mv.pv;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * KEYIN:是MR提供的程序读取到的一行的起始偏移量   Long
 * VALUEIN:是MR提供的程序读取到的一行的数据的内容 String
 * 
 * KEYOUT：是用户的逻辑处理方法处理完成之后返回的key的类型
 * VALUEOUT：是用户的逻辑处理方法处理完成之后返回的value的类型
 * 
 * 
 * Long  String Integer等类型不能在hadoop中使用
 * 
 * 分别对应LongWritable，Text，IntWritable
 *
 */
public class PvMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    /**
     * MR程序每读一行就调用一次这个map程序
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
