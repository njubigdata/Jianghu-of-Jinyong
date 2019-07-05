import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileReader;
import java.util.*;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class task1 {
    public static class task1Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        List<String> namelist = new ArrayList<String>();
        public void setup(Context context)
        {

            String file_path = task1.class.getClassLoader().getResource("people_name_list.txt").getPath();
            try
            {
                FileReader reader = new FileReader(file_path);
                BufferedReader br = new BufferedReader(reader);
                String name=null;
                while((name = br.readLine()) != null)
                {
                    //StringTokenizer itr = new StringTokenizer(line);
                    //String name = itr.nextToken();
                    namelist.add(name);
                    DicLibrary.insert(DicLibrary.DEFAULT, name);
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {

            String line = value.toString();
            Result result = ToAnalysis.parse(line); //分词结果的一个封装，主要是一个List<Term>的terms
            List<Term> termlist = result.getTerms();
            //System.out.println(termlist.toString());

            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            int index = Integer.valueOf(fileName.substring(2,4));

            String names = "";
            for(Term term:termlist)
            {
                if(term.getNatureStr().equals("nr")||term.getNatureStr().equals("userDefine")) {
                    if (namelist.contains(term.getName())) {
                        names += term.getName() + " ";
                    }
                }
            }

            if(!names.isEmpty()) {
                names = names.substring(0, names.length() - 1);
                Text text = new Text();
                text.set(names);
                context.write(text, new IntWritable(index));
            }

        }
    }
    public static class NewPartitioner extends HashPartitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            // TODO Auto-generated method stub
            int index = value.get();
            return index-1;
        }
    }
    public static class task1Reducer extends Reducer<Text, IntWritable, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Text Nothing = new Text();
            Nothing.set("");
            for(IntWritable val:values)
            {
                context.write(key, Nothing);
            }
        }
    }

    public static void main(String[] args) throws IOException{
        Configuration conf = new Configuration();
        Job job = new Job(conf,"inverted");
        job.setJarByClass(task1.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(task1Mapper.class);
        job.setPartitionerClass(NewPartitioner.class);
        job.setNumReduceTasks(15);
        job.setReducerClass(task1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        try {
            System.exit(job.waitForCompletion(true)?0:1);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
