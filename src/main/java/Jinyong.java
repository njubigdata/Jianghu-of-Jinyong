import java.io.*;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class Jinyong {
    public static void main(String[] args)throws IOException
    {
        Configuration conf = new Configuration();
        Job job1 = new Job(conf,"Jinyong");			    //指定任务名（Job Name）为”Jinyong“
        job1.setJarByClass(Jinyong.class);			            //设置任务jar的主类
        job1.setInputFormatClass(TextInputFormat.class);		//文本流输入及分片
        job1.setMapperClass(task1.Task1Mapper.class);		    //Mapper类
        job1.setReducerClass(task1.Task1Reducer.class);		    //Reducer类


        job1.setOutputKeyClass(Text.class);	                    //（Reducer）输出的key类型：Text（词语文本）
        job1.setOutputValueClass(Text.class);			        //	 value类型：Text（平均出现次数，文件1:词频1；文件2:词频2；……）
        job1.setMapOutputKeyClass(Text.class);                  //（Mapper）输出的key类型：Text（词语文本#文件名）
        job1.setMapOutputValueClass(IntWritable.class);	        //	 value类型：IntWritable（词频）

        FileInputFormat.addInputPath(job1, new Path(args[0]));  //外部参数1：待统计的数据文件所在路径
        Path output=new Path(args[1]);
        FileOutputFormat.setOutputPath(job1, output);           //外部参数2：输出结果路径（这两个参数在执行时从外部传入）

        try {
            job1.waitForCompletion(true);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
