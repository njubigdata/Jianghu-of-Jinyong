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

public class task3 {
    public static class task3Mapper extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer itr = new StringTokenizer(line);
            String name1, name2, counts;
            name1 = itr.nextToken();
            name2 = itr.nextToken();
            counts = itr.nextToken();
            Text word1 = new Text();
            Text word2 = new Text();
            word1.set(name1);
            word2.set(name2 + ","+counts);
            context.write(word1, word2);

        }
    }
    public static class task3Combiner extends Reducer<Text, Text, Text, Text>{
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            // super.reduce(arg0, arg1, arg2);
            int sum = 0;
            StringBuilder out = new StringBuilder();
            for(Text val:values) {
                String[] value = val.toString().split(",");
                sum += Integer.valueOf(value[1]);
                if(values.iterator().hasNext()) {
                    out.append(value[0] + " " + value[1] + "|");
                }
                else {
                    out.append(value[0] + " " + value[1]);
                }
            }
            out.append("," + sum);
            context.write(key, new Text(out.toString()));
        }
    }

    public static class task3Reducer extends Reducer<Text, Text, Text, Text>{
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            StringBuilder temp = new StringBuilder();
            for(Text val:values) {
                String[] value = val.toString().split(",");
                sum += Integer.valueOf(value[1]);
                if(values.iterator().hasNext()) {
                    temp.append(value[0] + "|");
                }
                else {
                    temp.append(value[0]);
                }
            }
            String[] valStrings = temp.toString().split("\\|");
            StringBuilder out = new StringBuilder();
            for(int i = 0; i < valStrings.length; i++) {
                String[] tempVal = valStrings[i].split(" ");
                if(i == 0) {
                    out.append(tempVal[0] + " " + (double)Integer.valueOf(tempVal[1])/sum);
                }
                else {
                    out.append("|" + tempVal[0] + " " + (double)Integer.valueOf(tempVal[1])/sum);
                }
            }
            //result.set(sum);
            context.write(key, new Text(out.toString()));
        }
    }

    public static void main(String[] args) throws IOException{
        Configuration conf = new Configuration();
        Job job = new Job(conf,"inverted");
        job.setJarByClass(task3.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(task3Mapper.class);
        job.setCombinerClass(task3Combiner.class);
        job.setReducerClass(task3Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

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
