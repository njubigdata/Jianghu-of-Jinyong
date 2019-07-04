package PageRank;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RankViewer {

    public static void rankViewer(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        Configuration conf = new Configuration();
        Job job = new Job(conf, "PageRank-PageRankIter");
        job.setJarByClass(RankViewer.class);
        job.setMapperClass(rankViewerMapper.class);
        job.setSortComparatorClass(decDoubleWritable.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }

    public static class rankViewerMapper extends Mapper<Object, Text, DoubleWritable, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] str = value.toString().split("\t");
            String name = str[0];
            double PR = Double.parseDouble(str[1]);
            context.write(new DoubleWritable(PR), new Text(name));
        }
    }

    public static class decDoubleWritable extends DoubleWritable.Comparator{
        public int compare(byte[] b1, int s1,int l1, byte[] b2, int s2, int l2){
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }

        public double compare(DoubleWritable a, WritableComparable<DoubleWritable> b){
            return -super.compare(a, b);
        }
    }
}
