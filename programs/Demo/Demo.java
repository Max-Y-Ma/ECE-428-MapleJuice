import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Demo {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    // Logging
    private static final org.apache.commons.logging.Log map_log =
      org.apache.commons.logging.LogFactory.getLog(TokenizerMapper.class);

    // Writable key and value
    private Text one = new Text("1");
    private Text detection = new Text();
    
    // "Detection_" csv index is number 9 
    private int detection_index = 9;

    // "Interconne" csv index is number 10
    private int interconne_index = 10;

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        
        // Retrieve argument 
        Configuration conf = context.getConfiguration();
        String interconne_type = conf.get("Interconne_Type");

        // Log argument
        map_log.info("Interconne_Type = " + interconne_type);

        // Log a message at the INFO level
        map_log.info("Mapper: Processing key = " + key + ", value = " + value);
        
        // Split input string by comma delimiter
        String[] fields = value.toString().split(",");

        if (fields.length < interconne_index) {
          return;
        }

        // Index "Interconne" and "Detection_" fields
        String interconne = fields[interconne_index];
        detection.set(fields[detection_index]);

        // Log Interconne
        map_log.info("Interconne = " + interconne);

        // Verify "Interconne" matches input argument
        if (interconne.equals(interconne_type)) {
          // Log Detection
          map_log.info("Detection = " + detection);
  
          // Output KV pair <1, "Detection_">
          context.write(one, detection);
        }

    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {

    // Logging
    private static final org.apache.commons.logging.Log reduce_log =
      org.apache.commons.logging.LogFactory.getLog(IntSumReducer.class);
       
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
	    
	    // Tally detection results
	    Double total = 0.0;
	    HashMap<String, Integer> detection_map = new HashMap<>();	    

      // Log a message at the INFO level
      reduce_log.info("Reduce: Processing key = " + key + ", value = " + values);
	    for (Text detection : values) {
        reduce_log.info("Detection Value = " + detection);

		    // Track detection 
		    if (detection_map.containsKey(detection.toString())) {
			    // Key is present, increment the current value
			    detection_map.put(detection.toString(), detection_map.get(detection.toString()) + 1);
		    } else {
			    // Key is not present, set the default value
			    detection_map.put(detection.toString(), 1);
        }
	
		    // Sum total number of detections
		    total += 1.0;
	    }
      reduce_log.info("Total=" + total);

	    // Output KV pairs
	    for (HashMap.Entry<String, Integer> entry : detection_map.entrySet()) {
        reduce_log.info("Key=" + entry.getKey() + ", Value=" + entry.getValue());
        reduce_log.info("Detection Percentage = " + (((entry.getValue() * 100.0) / total) + "%"));
		    context.write(new Text(entry.getKey()), new Text(((entry.getValue() * 100.0) / total) + "%"));
	    }
	  }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("Interconne_Type", args[2]);
    Job job = Job.getInstance(conf, "demo");
    job.setJarByClass(Demo.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
