import java.io.*;
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


public class task7 {

    public static class Task7_Mapper extends Mapper<LongWritable, Text, Text, Text>{

        static Map<String, List<String> > name_novels = new HashMap<>();
        protected void setup(Context context) throws IOException
        {
            String path = "/home/hadoop/test/task2/task1-output";//the path of task1-output
            File file = new File(path);
            File[] fs = file.listFiles();


            for(File f:fs)
            {
                String novel = f.getName().replace("-output.txt", "").replace("金庸","").replaceAll("\\d+","");

                FileReader reader = new FileReader(f);
                BufferedReader br = new BufferedReader(reader);

                String line;
                while((line = br.readLine())!=null)
                {
                    StringTokenizer name_list = new StringTokenizer(line, " ");
                    while(name_list.hasMoreTokens())
                    {
                        String name = name_list.nextToken().replace("\t", "");

                        List<String> books;
                        if((books = name_novels.get(name))==null)
                        {
                            List<String> new_books = new ArrayList<String>();
                            new_books.add(novel);
                            name_novels.put(name, new_books);
                        }
                        else
                        {
                            if (!books.contains(novel))
                            {
                                books.add(novel);
                                name_novels.put(name, books);
                            }
                        }

                    }
                }
            }
            System.out.println(name_novels.size());
            System.out.println(name_novels);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String str = value.toString();

            String Label = str.split("\t")[0];
            StringTokenizer name_list = new StringTokenizer(str.split("\t")[1], " ");
            while(name_list.hasMoreTokens()) {
                String name = name_list.nextToken();
                List<String> novels = name_novels.get(name);

                for (String novel : novels)
                {
                    System.out.println(Label + "\t" + name + "#" + novel);
                    context.write(new Text(Label), new Text(name + "#" + novel));
                }
            }
        }
    }

    public static class Task7_Reducer extends Reducer<Text, Text, Text, Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Map<String, Integer> novel_votes = new HashMap<>();

            System.out.println(key.toString());

            List<String> name_list = new ArrayList<String>();
            String namelist = "";
            for(Text val:values) {
                String str = val.toString();
                System.out.println(str);
                String name = str.split("#")[0];
                String novel = str.split("#")[1];

                if (!name_list.contains(name))
                {
                    name_list.add(name);
                    namelist = namelist + name + " ";
                }

                Integer vote;
                if((vote = novel_votes.get(novel))==null)
                {
                    novel_votes.put(novel, 1);
                }
                else
                {
                    novel_votes.put(novel, vote+1);
                }
            }

            System.out.println(novel_votes);

            Integer max_vote = 0;
            String book = "";
            for(Map.Entry<String, Integer> pair : novel_votes.entrySet())
            {
                if(max_vote < pair.getValue())
                {
                    max_vote = pair.getValue();
                    book = pair.getKey();
                }
            }

            System.out.println(book + "\t" + namelist);
            context.write(new Text(book), new Text(namelist));
        }
    }


    public static void main(String[] args) throws IOException
    {

        Configuration conf = new Configuration();
        Job job = new Job(conf, "task7");
        job.setJarByClass(task7.class);			//设置任务jar的主类
        job.setInputFormatClass(TextInputFormat.class);		//文本流输入及分片
        job.setMapperClass(Task7_Mapper.class);		//Mapper类
        job.setReducerClass(Task7_Reducer.class);	    //Reducer类


        job.setOutputKeyClass(Text.class);	//（Reducer）输出的key类型：Text（词语文本）
        job.setOutputValueClass(Text.class);			//	 value类型：Text（平均出现次数，文件1:词频1；文件2:词频2；……）
        job.setMapOutputKeyClass(Text.class);//（Mapper）输出的key类型：Text（词语文本#文件名）
        job.setMapOutputValueClass(Text.class);	//	 value类型：IntWritable（词频）


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

    }

}










