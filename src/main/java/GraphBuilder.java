package PageRank;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GraphBuilder {

    public static void graphBuilder(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "PageRank-GraphBuilder");
        job.setJarByClass(GraphBuilder.class);
        job.setMapperClass(graphBuilderMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }

    public static class graphBuilderMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String initPR = "1.0";
            String str = value.toString();
            Text name = new Text(str.split("\t")[0]);
            Text tuple = new Text(str.split("\t")[1]);
            Text url = new Text(name);
            Text link_list = new Text(initPR + "\t" + tuple);
            context.write(url, link_list);
        }
    }
}
