import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;


import com.google.common.collect.Iterables;
import com.kenai.jaffl.annotations.In;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
//import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
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

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.ansj.domain.Term;
import org.ansj.domain.Result;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.jruby.RubyProcess;

import java.io.File;



public class task5 {

    static int init_label=0;

    public static class Task5_prepare_Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String str = value.toString();
            str = str.replace("[", "").replace("]", "");
            str = str.replace("\t", "#");
            //System.out.println(str);

            IntWritable label = new IntWritable();
            init_label++;
            label.set(init_label);

            Text text = new Text();
            text.set(label.toString()+"#"+str);

            context.write(text, new IntWritable(0));
        }

    }

    public static class Task5_prepare_Reducer extends Reducer<Text, IntWritable, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            for(IntWritable val:values)
            {
                context.write(key, new Text());
            }
        }
    }

    //
    //
    //

    public static class Task5_LP_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String str = value.toString();
            str = str.replace("\t", "");
            String label = str.split("#")[0];
            String src_name = str.split("#")[1];
            String link_list = str.split("#")[2];

            StringTokenizer name_val_list = new StringTokenizer(link_list,"|");
            while(name_val_list.hasMoreTokens())
            {
                String t = name_val_list.nextToken();
                String dst_name = t.split(" ")[0];

                context.write(new Text(dst_name), new Text(label+"#"+src_name));
                //System.out.println(dst_name+"    "+label+"#"+src_name);
            }

            context.write(new Text(src_name), new Text("@"+link_list));

        }
    }

    public static class Task5_LP_Reducer extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String link_list = "";
            String this_name = "";
            Map<String, String> name_To_label = new HashMap<>();
            for(Text value:values)
            {
                String str = value.toString();
                if(str.charAt(0)=='@')
                {
                    this_name = key.toString();
                    link_list = str.replace("@", "");
                }
                else
                {
                    String label = str.split("#")[0];
                    String src_name = str.split("#")[1];

                    name_To_label.put(src_name, label);
                }
            }

            //System.out.println("name-label:"+name_To_label);
            //System.out.println();

            Map<String, Float> name_To_val = new HashMap<>();
            StringTokenizer name_val_list = new StringTokenizer(link_list,"|");
            while(name_val_list.hasMoreTokens())
            {
                String t = name_val_list.nextToken();
                String name = t.split(" ")[0];
                Float val =  Float.parseFloat(t.split(" ")[1]);

                name_To_val.put(name, val);
            }

            //System.out.println("name-val"+name_To_val);
            //System.out.println();

            Map<String, Float> label_To_sum = new HashMap<>();
            name_val_list = new StringTokenizer(link_list,"|");
            while(name_val_list.hasMoreTokens())
            {
                String t = name_val_list.nextToken();
                String name = t.split(" ")[0];
                Float val =  Float.parseFloat(t.split(" ")[1]);

                Float f;
                if((f=label_To_sum.get(name_To_label.get(name))) == null)
                {
                    label_To_sum.put(name_To_label.get(name), val);
                }
                else
                {
                    label_To_sum.put(name_To_label.get(name), f+val);
                }
            }

            //System.out.println("label-sum"+label_To_sum);
            //System.out.println();


            float max_val = -1;
            String new_label = "";
            for (Map.Entry<String, Float> pair : label_To_sum.entrySet())
            {
                if(max_val<pair.getValue())
                {
                    max_val = pair.getValue();
                    new_label = pair.getKey();
                }
            }

            context.write(new Text(new_label+"#"+this_name+"#"+link_list), new Text());

        }
    }


    public static class Task5_Partition_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String str = value.toString();
            String label = str.split("#")[0];
            String src_name = str.split("#")[1];
            String link_list = str.split("#")[2];

            node_bw.write(label+", "+src_name+"\n");

            StringTokenizer name_val_list = new StringTokenizer(link_list,"|");
            while(name_val_list.hasMoreTokens())
            {
                String t = name_val_list.nextToken();
                String dst_name = t.split(" ")[0];
                String val =  t.split(" ")[1];

                edge_bw.write(src_name+", "+dst_name+"\n");
            }

            context.write(new Text(label), new Text(src_name));
        }
    }

    public static class Task5_Partition_Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //System.out.println("Label:"+key.toString()+"  "+ Iterables.size(values));
            //System.out.println();
            label_num++;
            for (Text value:values)
            {
                context.write(new Text(key), new Text(value));
            }
        }
    }


    static int label_num = 0;

    static BufferedWriter node_bw;
    static BufferedWriter edge_bw;

    public static void main(String[] args) throws IOException
    {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Jianghu");
        job.setJarByClass(InvertedIndexer.class);			//设置任务jar的主类
        job.setInputFormatClass(TextInputFormat.class);		//文本流输入及分片
        job.setMapperClass(Task5_prepare_Mapper.class);		//Mapper类
        job.setReducerClass(Task5_prepare_Reducer.class);	    //Reducer类


        job.setOutputKeyClass(Text.class);	//（Reducer）输出的key类型：Text（词语文本）
        job.setOutputValueClass(Text.class);			//	 value类型：Text（平均出现次数，文件1:词频1；文件2:词频2；……）
        job.setMapOutputKeyClass(Text.class);//（Mapper）输出的key类型：Text（词语文本#文件名）
        job.setMapOutputValueClass(IntWritable.class);	//	 value类型：IntWritable（词频）


        FileInputFormat.addInputPath(job, new Path(args[0]));//外部参数1：待统计的数据文件所在路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//外部参数2：输出结果路径（这两个参数在执行时从外部传入）

        try {
            job.waitForCompletion(true);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        System.out.println(init_label);



        int n = 10;
        for(int i=0;i<n;) {

            Configuration conf2 = new Configuration();
            Job job2 = new Job(conf2, "LP");
            job2.setJarByClass(InvertedIndexer.class);            //设置任务jar的主类
            job2.setInputFormatClass(TextInputFormat.class);        //文本流输入及分片
            job2.setMapperClass(Task5_LP_Mapper.class);        //Mapper类
            job2.setReducerClass(Task5_LP_Reducer.class);        //Reducer类


            job2.setOutputKeyClass(Text.class);    //（Reducer）
            job2.setOutputValueClass(Text.class);
            job2.setMapOutputKeyClass(Text.class);//（Mapper）
            job2.setMapOutputValueClass(Text.class);


            FileInputFormat.addInputPath(job2, new Path("hdfs://localhost:9000/LP" + i));//外部参数
            System.out.println("Inpath: LP" + i);
            i++;
            FileOutputFormat.setOutputPath(job2, new Path("hdfs://localhost:9000/LP" + i));
            System.out.println("Outpath: LP" + i);

            try {
                job2.waitForCompletion(true);
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            label_num = 0;
        }



        File node_file = new File("node.csv");
        FileWriter node_fw = new FileWriter(node_file);
        node_bw = new BufferedWriter(node_fw);
        node_bw.write("modularity_class, Id\n");

        File edge_file = new File("edge.csv");
        FileWriter edge_fw = new FileWriter(edge_file);
        edge_bw = new BufferedWriter(edge_fw);
        edge_bw.write("Source, Target\n");


        Configuration conf3 = new Configuration();
        Job job3 = new Job(conf3, "Partition");
        job3.setJarByClass(InvertedIndexer.class);            //设置任务jar的主类
        job3.setInputFormatClass(TextInputFormat.class);        //文本流输入及分片
        job3.setMapperClass(Task5_Partition_Mapper.class);        //Mapper类
        job3.setReducerClass(Task5_Partition_Reducer.class);        //Reducer类


        job3.setOutputKeyClass(Text.class);    //（Reducer）
        job3.setOutputValueClass(Text.class);
        job3.setMapOutputKeyClass(Text.class);//（Mapper）
        job3.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path("hdfs://localhost:9000/LP10"));//外部参数
        FileOutputFormat.setOutputPath(job3, new Path("hdfs://localhost:9000/book"));
        System.out.println("book");

        try {
            job3.waitForCompletion(true);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        node_bw.close();
        edge_bw.close();
        node_fw.close();
        edge_fw.close();

        System.out.println("label_num: "+label_num);

    }

}










