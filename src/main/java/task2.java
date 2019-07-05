import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class task2 {
    public static class task2Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer itr = new StringTokenizer(line);
            ArrayList<String> names = new ArrayList<>();
            for(; itr.hasMoreTokens();) {
                String name = itr.nextToken();
                if(!names.contains(name)) {
                    names.add(name);
                }
            }
            int len = names.size();
            for(int i = 0; i < len; i++) {
                for(int j = i+1; j < len; j++){
                    Text word1 = new Text();
                    Text word2 = new Text();
                    word1.set(names.get(i)+" "+names.get(j));
                    word2.set(names.get(j)+" "+names.get(i));
                    context.write(word1, new IntWritable(1));
                    context.write(word2, new IntWritable(1));
                }
            }
        }
    }
    public static class task2Combiner extends Reducer<Text, IntWritable, Text, IntWritable>{
        private IntWritable result = new IntWritable();
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            // super.reduce(arg0, arg1, arg2);
            int sum = 0;
            for(IntWritable val:values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    public static class task2Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        private IntWritable result = new IntWritable();
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val:values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws IOException{
        Configuration conf = new Configuration();
        Job job = new Job(conf,"inverted");
        job.setJarByClass(task2.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(task2Mapper.class);
        job.setCombinerClass(task2Combiner.class);
        job.setReducerClass(task2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
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
