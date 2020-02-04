package com.pyq.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Yangqi.Pang
 * @date 2020-02-02 17:02
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    /**
     * @param key 行首偏移量，字节数，意义不大
     * @param value 一行文本
     * @param context context
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] arr =  line.split(" ");
        Text keyOut = new Text();
        IntWritable valueOut = new IntWritable(1);
        for (String word : arr) {
            keyOut.set(word);
            context.write(keyOut,valueOut);
        }
    }
}
