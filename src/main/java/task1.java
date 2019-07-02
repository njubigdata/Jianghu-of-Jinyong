import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileReader;
import java.util.*;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class task1 {
    public static class Task1Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        List<String> namelist = new ArrayList<String>();
        public void setup(Context context)throws IOException,InterruptedException
        {

            String file_path = task1.class.getClassLoader().getResource("people_name_list.txt").getPath();
            try
            {
                FileReader reader = new FileReader(file_path);
                BufferedReader br = new BufferedReader(reader);
                String name=null;
                while((name = br.readLine()) != null)
                {
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
                context.write(text, new IntWritable(0));
            }

        }
    }

    public static class Task1Reducer extends Reducer<Text, IntWritable, Text, Text>{
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

}
