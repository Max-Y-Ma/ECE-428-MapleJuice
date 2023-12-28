import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.*;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Filter {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    // Logging
    private static final org.apache.commons.logging.Log map_log =
      org.apache.commons.logging.LogFactory.getLog(TokenizerMapper.class);

    // Writable key and value
    private Text one = new Text("1");
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        
        // Retrieve argument 
        Configuration conf = context.getConfiguration();
        String regex = conf.get("Regex");

        // Log argument
        map_log.info("Regex = " + regex);

        // Log a message at the INFO level
        map_log.info("Mapper: Processing key = " + key + ", value = " + value);
        
        // Format regular expression
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(value.toString());

        // Output line if it matches the given regex
        if (matcher.find()) {
          // Output KV pair <1, line>
          context.write(one, value);
        }

    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,NullWritable,Text> {

    // Logging
    private static final org.apache.commons.logging.Log reduce_log =
      org.apache.commons.logging.LogFactory.getLog(IntSumReducer.class);
       
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
	    
        // Log a message at the INFO level
        reduce_log.info("Reduce: Processing key = " + key + ", value = " + values);

        String regex_lines = new String();
	    for (Text line : values) {
            reduce_log.info("Line = " + line);
            regex_lines += line + "\n";
        }
	
        context.write(NullWritable.get(), new Text(regex_lines));
	  }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("Regex", args[2]);
    Job job = Job.getInstance(conf, "filter");
    job.setJarByClass(Filter.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
