import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class exercise2 extends Configured implements Tool
{

  public static class IGramMap extends Mapper <Object, Text, Text, Text>
  {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
      String[] input = value.toString().split("\\s+");

      if (input[1].matches("^[0-9]{4}$"))  
      {
        String vol = input[3];
        String val = vol + ","+ String.valueOf(Math.pow(Double.parseDouble(vol),2));
        context.write(new Text(), new Text(val));
      }
    }
  }


  public static class IIGramMap extends Mapper <Object, Text, Text, Text>
  {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
      String[] input = value.toString().split("\\s+");

      if (input[2].matches("^[0-9]{4}$"))  
      {
        String vol = input[4];
        String val = vol + ","+ String.valueOf(Math.pow(Double.parseDouble(vol),2));
        context.write(new Text(), new Text(val));
      }
    }
  }

  public static class Combine extends Reducer <Text, Text, Text, Text>
  {
    //String line=null;

    public void reduce(Text k, Iterable<Text> values, Context context)
    throws IOException, InterruptedException
    {
    
      double count = 0.0;
      double sum = 0.0;
      double sum_squares = 0.0;

      for (Text value:values) 
      {
          String[] input = value.toString().split(","); 
          double num = Double.parseDouble(input[0]);
          double sqr = Double.parseDouble(input[1]);
          sum += num;
          count += 1;
          sum_squares += sqr;
      }   
      context.write(k, new Text(Double.toString(count) + "," + Double.toString(sum) + "," + Double.toString(sum_squares)));
    }
  }

  public static class Reduce extends Reducer <Text, Text, Text, DoubleWritable>
  {
  //String line=null;

    public void reduce(Text k, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
      double count = 0.0;
      double sum = 0.0;
      double sum_squares = 0.0;

      for (Text value:values)
      {
        String[] input = value.toString().split(","); 
        double num = Double.parseDouble(input[0]);
        double s = Double.parseDouble(input[1]);
        double sqr = Double.parseDouble(input[2]);

        count += num;
        sum += s;
        sum_squares += sqr;
      }

     double average = sum/count;
     double stdev = Math.sqrt((sum_squares - count * average * average)/count);

     context.write(k, new DoubleWritable(stdev));
    }
  }


  public int run(String[] args) throws Exception 
  {
    Configuration conf = getConf();

    Job job = new Job(conf, "exercise2");
    job.setJarByClass(exercise2.class);

    job.setCombinerClass(Combine.class);
    job.setReducerClass(Reduce.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

    job.setNumReduceTasks(1);

    MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,IGramMap.class);
    MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,IIGramMap.class);
    FileOutputFormat.setOutputPath(job, new Path(args[2]));

    job.setOutputFormatClass(TextOutputFormat.class);

    return (job.waitForCompletion(true) ? 0 : 1);

  }

  public static void main(String[] args) throws Exception 
  {
    int res = ToolRunner.run(new Configuration(), new exercise2(), args);
    System.exit(res);
  }

}

