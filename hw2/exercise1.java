import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
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


public class exercise1 extends Configured implements Tool 
{
	
	public static class IGramMap extends Mapper <Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] input = value.toString().split("\\s+");

			if (input[1].matches("^[0-9]{4}$"))  
			{
				if (input[0].toLowerCase().contains("nu")) 
				{
					String Key = input[1] + "," + "nu" + ",";
					context.write(new Text(Key), new Text(input[2] + "," + input[3]));   
				}
				if (input[0].toLowerCase().contains("chi")) 
				{
					String Key = input[1] + "," + "chi" + ",";
					context.write(new Text(Key), new Text(input[2] + "," + input[3]));   
				}
				if (input[0].toLowerCase().contains("haw")) 
				{
					String Key = input[1] + "," + "haw" + ",";   
					context.write(new Text(Key), new Text(input[2] + "," + input[3]));   
				}
			}
		}
	}


	public static class IIGramMap extends Mapper <Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] input = value.toString().split("\\s+");

			if (!(input[2].matches("^[0-9]{4}$")))  
			{
				if (input[0].toLowerCase().contains("nu") || input[1].toLowerCase().contains("nu")) 
				{
					String Key = input[2] + "," + "nu" + ",";
					context.write(new Text(Key), new Text(input[3] + "," + input[4]));   
				}
				if (input[0].toLowerCase().contains("chi") || input[1].toLowerCase().contains("chi")) 
				{
					String Key = input[2] + "," + "chi" + ",";
					context.write(new Text(Key), new Text(input[3] + "," + input[4]));                 
				}
				if (input[0].toLowerCase().contains("haw") || input[1].toLowerCase().contains("haw")) 
				{
					String Key = input[2] + "," + "haw" + ",";
					context.write(new Text(Key), new Text(input[3] + "," + input[4])); 
				}
			}
		}
	}

	public static class Combine extends Reducer <Text, Text, Text, Text>
	{
		//String line = null;

		public void reduce(Text k, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{

			double sum = 0.0; // occurrances
			double count = 0.0; // volumes

			for (Text value:values)
			{
				String[] input = value.toString().split(",");

				// occurrances
				sum = sum + Double.parseDouble(input[0]);
				// volumes
				count = count + Double.parseDouble(input[1]);

			}

		context.write(k, new Text(Double.toString(sum) + "," + Double.toString(count)));
		}
	}

	public static class Reduce extends Reducer <Text, Text, Text, DoubleWritable>
	{
	  //String line=null;

		public void reduce(Text k, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{

			double sum = 0.0; // occurrances
			double count = 0.0; // volumes

			for (Text value:values)
			{

				String[] input = value.toString().split(","); //input from combiner

				// occurrances
				sum = sum + Double.parseDouble(input[0]);
				// volumes
				count = count + Double.parseDouble(input[1]);

			}

			double average = sum/count; 

		context.write(k, new DoubleWritable(average));
		}
	}


	public int run(String[] args) throws Exception 
	{
		Configuration conf = getConf();

		Job job = new Job(conf, "exercise1");
		job.setJarByClass(exercise1.class);

		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setNumReduceTasks(1);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, IGramMap.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, IIGramMap.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setOutputFormatClass(TextOutputFormat.class);

		return (job.waitForCompletion(true) ? 0 : 1);

	}

	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new exercise1(), args);
		System.exit(res);
	}

}


