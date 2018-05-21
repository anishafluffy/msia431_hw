import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.util.*;

public class exercise3 extends Configured implements Tool {

public static class Map extends Mapper<Object,Text,Text,Text> 
{
  public void map(Object key, Text value, Context context) throws IOException, InterruptedException
  {
    String[] input = value.toString().split(",");

    //if column 165 is year format
    if (input[165].matches("\\d{4}$"))  
    {
      Integer year = Integer.parseInt(input[165]);

      if (year >= 2000 && year <= 2010) 
      {
        String title = input[1];
        String artist = input[2];
        String duration = input[3];

        String str = title + "," + artist + "," + duration + ",";
        
        context.write(new Text(""), new Text(str));
          
      }
    }
  }
}
    
public int run(String[] args) throws Exception
{
  Configuration conf = getConf();
    
  Job job = new Job(conf, "exercise3");
  job.setJarByClass(exercise3.class);
     
  job.setMapperClass(Map.class);
    
  job.setMapOutputKeyClass(Text.class);
  job.setMapOutputValueClass(Text.class);

  job.setNumReduceTasks(0);
  
  FileInputFormat.setInputPaths(job, new Path(args[0]));
  FileOutputFormat.setOutputPath(job, new Path(args[1]));

  job.setInputFormatClass(TextInputFormat.class);
  job.setOutputFormatClass(TextOutputFormat.class);
    
  return job.waitForCompletion(true) ? 0 : 1;
}
   
public static void main(String args[]) throws Exception
  {
    int res = ToolRunner.run(new Configuration(), new exercise3(), args);
    System.exit(res);
  }
}


