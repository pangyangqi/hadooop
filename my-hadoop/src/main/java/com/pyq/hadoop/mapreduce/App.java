package com.pyq.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Yangqi.Pang
 * @date 2020-02-04 08:01
 */
public class App {
    public static void main(String[] args) throws Exception {

        if(args == null&& args.length < 2){
            throw new Exception("参数不够，需要两个参数");
        }

        Configuration conf = new Configuration();

        FileSystem fileSystem = FileSystem.get(conf);

        fileSystem.delete(new Path(args[1]),true);

        //创建job
        Job job = Job.getInstance();
        job.setJobName("word_count_name");
        job.setJarByClass(App.class);

        //添加输入路径
        FileInputFormat.addInputPath(job,new Path(args[0]));
        //设置输出路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //设置Map类
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReduce.class);
        //设置Reduce类
        job.setReducerClass(WordCountReduce.class);
        //设置KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置Mr的作业数
        job.setNumReduceTasks(1);
        //设置开始作业
        job.waitForCompletion(true);
    }
}
