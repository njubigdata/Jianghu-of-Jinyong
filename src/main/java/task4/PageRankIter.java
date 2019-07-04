package PageRank;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class PageRankIter {

    public static void pageRankIter(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "PageRank-PageRankIter");
        job.setJarByClass(PageRankIter.class);
        job.setMapperClass(pageRankIterMapper.class);
        job.setReducerClass(pageRankIterReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }

    public static class pageRankIterMapper extends Mapper<LongWritable, Text, Text, Text>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String[] str = value.toString().split("\t");
            if(str.length <= 2)
                return;

            Text name = new Text(str[0]);
            double cur_rank = Double.parseDouble(str[1]);
            String[] links = str[2].split("\\|");

            for(String link : links){
                Text sub_name = new Text(link.split(" ")[0]);
                double sub_cur_rank = Double.parseDouble(link.split(" ")[1]);
                double cur_value = cur_rank * sub_cur_rank;
                Text sub_value = new Text(String.valueOf(cur_rank) + " " + String.valueOf(cur_value));
                context.write(sub_name, sub_value);
            }

            context.write(name, new Text(str[2]));
        }
    }

    public static class pageRankIterReducer extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            String links = "";
            double PR = 0.0;
            //name 1 0.5
            //name name1 0.333|
            for(Text text:values){
                String link = text.toString();
                if(link.charAt(0) != '0' && link.charAt(0) != '1' && link.charAt(0) != '2' && link.charAt(0) != '3'
                && link.charAt(0) != '4' && link.charAt(0) != '5' && link.charAt(0) != '6' && link.charAt(0) != '7'
                && link.charAt(0) != '8' && link.charAt(0) != '9')
                    links = link;
                else
                    PR += Double.parseDouble(link.split(" ")[1]);
            }
            context.write(new Text(key), new Text(String.valueOf(PR) + "\t" + links));
        }
    }
}
