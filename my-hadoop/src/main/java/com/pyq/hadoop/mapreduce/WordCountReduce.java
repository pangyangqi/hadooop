package com.pyq.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Yangqi.Pang
 * @date 2020-02-02 17:58
 */
public class WordCountReduce extends Reducer<Text,IntWritable, Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable iw : values){
            count = count +  iw.get();
        }
        context.write(key,new IntWritable(count));
    }
}
