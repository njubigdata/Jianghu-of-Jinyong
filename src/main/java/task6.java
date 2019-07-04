import org.ansj.library.DicLibrary;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class task6 {
    public static class task6_LP_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String number=line.split("\t")[0];
            String name=line.split("\t")[1];
            context.write(new Text(number),new Text(name));
        }

    }
    public static class task6_LP_Reducer extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text namelist = new Text();
            namelist.set("");
            StringBuilder out=new StringBuilder();
            for(Text val:values)
            {
                out.append(val+" ");

            }
            context.write(key,new Text(out.toString()));
        }
    }



}
