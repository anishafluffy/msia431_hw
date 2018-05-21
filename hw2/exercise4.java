import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.util.*;

public class exercise4 extends Configured implements Tool
{

  public static class Map extends Mapper<Object, Text, Text, DoubleWritable>
  {

    public void map(Object key, Text value, Context context)  throws IOException, InterruptedException 
    {
      String[] input = value.toString().split(",");
      String artist = input[2];
      double duration = Double.parseDouble(input[3]);
      
      context.write(new Text(artist), new DoubleWritable(duration));
      
    }
  }


  public static class ArtistPartitioner extends Partitioner <Text, DoubleWritable>
  {
    public int getPartition(Text key, DoubleWritable value, int numReduceTasks)
    {
      String artist = key.toString();
      if (artist.substring(0,1).compareTo("F") < 0) {
        return 0;
      } else if (artist.substring(0,1).compareTo("K") < 0) {
        return 1;
      } else if (artist.substring(0,1).compareTo("P") < 0) {
        return 2;
      } else if (artist.substring(0,1).compareTo("U") < 0) {
        return 3;
      } else {
        return 4;
      }
    }
  }

  public static class Reduce extends Reducer <Text, DoubleWritable, Text, DoubleWritable>
  {
    public void reduce(Text key, Iterable <DoubleWritable> values, Context context) throws IOException, InterruptedException
    {
      double max = 0;

      for (DoubleWritable val: values) {
            if (val.get() > max) {
              max = val.get();
            }
        }
      context.write(key, new DoubleWritable(max));
      
    }
  }


   public int run(String[] args) throws Exception
   {
      Configuration conf = getConf();

      Job job = new Job(conf, "exercise4");
      job.setJarByClass(exercise4.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
      job.setPartitionerClass(ArtistPartitioner.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(DoubleWritable.class);
      
      job.setNumReduceTasks(5);

      FileInputFormat.setInputPaths(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      
      return job.waitForCompletion(true) ? 0 : 1;
   }

   public static void main(String[] args) throws Exception
   {
      int res = ToolRunner.run(new Configuration(), new exercise4(), args);
      System.exit(res);
   }
 }          


