

    static int init_label=0;

    public static class Task5_prepare_Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String str = value.toString();
            str = str.replace("[","").replace("]","");
            str = str.replace(" ","#");
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

    
    //------------------------------
    

    public static class Task5_LP_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String str = value.toString();
            String label = str.split("#")[0];
            String src_name = str.split("#")[1];
            String link_list = str.split("#")[2];

            StringTokenizer name_val_list = new StringTokenizer(link_list,"|");
            while(name_val_list.hasMoreTokens())
            {
                String t = name_val_list.nextToken();
                String dst_name = t.split(",")[0];

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
                String name = t.split(",")[0];
                Float val =  Float.parseFloat(t.split(",")[1]);

                name_To_val.put(name, val);
            }

            //System.out.println("name-val"+name_To_val);
            //System.out.println();

            Map<String, Float> label_To_sum = new HashMap<>();
            name_val_list = new StringTokenizer(link_list,"|");
            while(name_val_list.hasMoreTokens())
            {
                String t = name_val_list.nextToken();
                String name = t.split(",")[0];
                Float val =  Float.parseFloat(t.split(",")[1]);

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
