import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Join {

    public static class Mapper1
        extends Mapper<LongWritable, Text, Text, Text>{

        // Logging
        private static final org.apache.commons.logging.Log map_log =
        org.apache.commons.logging.LogFactory.getLog(Mapper1.class);

        // Writable key and value
        private Text data_id1 = new Text("1");
        
        public void map(LongWritable key, Text value, Context context
                        ) throws IOException, InterruptedException {
            
            // Log a message at the INFO level
            map_log.info("Mapper: Processing key = " + key + ", value = " + value);

            // Grab field index for dataset 1
            Configuration conf = context.getConfiguration();
            Integer field1_index = Integer.parseInt(conf.get("Field1_Index"));
            map_log.info("Field1_Index = " + field1_index);

            // Grab entry based on field index
            String[] fields = value.toString().split(",");
            String entry = fields[field1_index];
            map_log.info("Entry = " + entry);

            // Format output value
            String line_id_pair = data_id1.toString() + "$" + value.toString();
            map_log.info("Pair = " + line_id_pair);
            
            // Output KV pair <entry, <1, line>>
            context.write(new Text(entry), new Text(line_id_pair));
        }
    }

    public static class Reducer1
        extends Reducer<Text, Text, NullWritable, Text>{

        // Logging
        private static final org.apache.commons.logging.Log reduce_log =
        org.apache.commons.logging.LogFactory.getLog(Reducer1.class);

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

        // The identity reducer simply emits each key-value pair as a string line 
        for (Text value : values) {
                reduce_log.info("Key = " + key);
                reduce_log.info("Value = " + value);
                String entry_line = new String(key.toString() + '$' + value);
                context.write(NullWritable.get(), new Text(entry_line));
            }
        }
    }

    public static class Mapper2
       extends Mapper<LongWritable, Text, Text, Text>{

        // Logging
        private static final org.apache.commons.logging.Log map_log =
        org.apache.commons.logging.LogFactory.getLog(Mapper2.class);

        // Writable key and value
        private Text data_id2 = new Text("2");
        
        public void map(LongWritable key, Text value, Context context
                        ) throws IOException, InterruptedException {
            
            // Log a message at the INFO level
            map_log.info("Mapper: Processing key = " + key + ", value = " + value);

            // Grab field index for dataset 1
            Configuration conf = context.getConfiguration();
            Integer field2_index = Integer.parseInt(conf.get("Field2_Index"));
            map_log.info("Field2_Index = " + field2_index);

            // Grab entry based on field index
            String[] fields = value.toString().split(",");
            String entry = fields[field2_index];
            map_log.info("Entry = " + entry);

            // Format output value
            String line_id_pair = data_id2.toString() + "$" + value.toString();
            map_log.info("Pair = " + line_id_pair);
            
            // Output KV pair <entry, <1, line>>
            context.write(new Text(entry), new Text(line_id_pair));
        }
    }

    public static class Reducer2
        extends Reducer<Text, Text, NullWritable, Text>{

        // Logging
        private static final org.apache.commons.logging.Log reduce_log =
        org.apache.commons.logging.LogFactory.getLog(Reducer2.class);

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

        // The identity reducer simply emits each key-value pair as a string line 
        for (Text value : values) {
                reduce_log.info("Key = " + key);
                reduce_log.info("Value = " + value);
                String entry_line = new String(key.toString() + '$' + value);
                context.write(NullWritable.get(), new Text(entry_line));
            }
        }
    }

    public static class Mapper3
            extends Mapper<LongWritable, Text, Text, Text>{

        // Logging
        private static final org.apache.commons.logging.Log map_log =
        org.apache.commons.logging.LogFactory.getLog(Mapper3.class);

        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Log a message at the INFO level
            map_log.info("Mapper: Processing key = " + key + ", value = " + value);

            // The identity mapper simply seperates the entry from the input key-value pair
            String[] values = value.toString().split("\\$");
            String entry = values[0];
            String dataset_line = values[1] + '$' + values[2];

            context.write(new Text(entry), new Text(dataset_line));
        }
    }

    public static class Reducer3
        extends Reducer<Text,Text,Text,Text> {

        // Logging
        private static final org.apache.commons.logging.Log reduce_log =
        org.apache.commons.logging.LogFactory.getLog(Reducer3.class);
        
        public void reduce(Text key, Iterable<Text> values,
                        Context context
                        ) throws IOException, InterruptedException {
            
            // Log a message at the INFO level
            reduce_log.info("Reduce: Processing key = " + key + ", value = " + values);

            List<String> dataset1_lines = new ArrayList<>();
            List<String> dataset2_lines = new ArrayList<>();

            // Separate values from different datasets
            for (Text value : values) {
                reduce_log.info("Value = " + value);
                String[] pair = value.toString().split("\\$");
                String dataset_id = pair[0];
                String line = pair[1];
                reduce_log.info("D_id = " + dataset_id);
                reduce_log.info("Line = " + line);

                if (dataset_id.equals("1")) {
                    dataset1_lines.add(line);
                } else if (dataset_id.equals("2")) {
                    dataset2_lines.add(line);
                }
            }

            reduce_log.info("D1_lines = " + dataset1_lines + ", D2_lines = " + dataset2_lines);

            // Output join key value pairs
            for (String line1 : dataset1_lines) {
                for (String line2 : dataset2_lines) {
                    context.write(key, new Text(line1 + "\t" + line2));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // Parameter Configuration
        Configuration conf1 = new Configuration();
        conf1.set("Field1_Index", args[5]);
        
        // Configure Job1 : Read Dataset 1
        Job job1 = Job.getInstance(conf1, "Job1");
        job1.setJarByClass(Join.class);
        job1.setMapperClass(Mapper1.class);
        job1.setReducerClass(Reducer1.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        // Job1 Input and Temporary Output Directory
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Configure Job2 : Read Dataset 2
        Configuration conf2 = new Configuration();
        conf2.set("Field2_Index", args[6]);

        Job job2 = Job.getInstance(conf2, "Job2");
        job2.setJarByClass(Join.class);
        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        // Job1 Input and Temporary Output Directory
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }
        
        // Job 3: Reducer SQL Join
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "Job3");

        job3.setJarByClass(Join.class);
        job3.setMapperClass(Mapper3.class);
        job3.setReducerClass(Reducer3.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        // Job 3 Input and Final Output Directory
        FileInputFormat.addInputPath(job3, new Path(args[2]));
        FileInputFormat.addInputPath(job3, new Path(args[3]));
        FileOutputFormat.setOutputPath(job3, new Path(args[4]));

        // Job Completion Setting
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
