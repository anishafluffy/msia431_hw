import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class exercise2 extends Configured implements Tool 
{

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> 
	{

		private static FloatWritable col4 = new FloatWritable();
		private Text combination = new Text();

		public void configure(JobConf job) {
		}

		protected void setup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException 
		{
			String[] line = value.toString().split(",");

			if (line[line.length - 1].equals("false")) {
				col4.set(Float.parseFloat(line[3]));
				String col30 = line[29];
				String col31 = line[30];
				String col32 = line[31];
				String col33 = line[32];

				combination.set(col30 + "," + col31 + "," + col32 + "," + col33);
				output.collect(combination, col4);

			}

		}

		protected void cleanup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
		}
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable> 
	{

		public void configure(JobConf job) {
		}

		protected void setup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
		}

		public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException 
		{		  
			float sum = 0;
			int count = 0;
			float average = 0;
			while (values.hasNext()) {
				sum += values.next().get();
				count += 1;
			}
			average = sum/count;
			output.collect(key, new FloatWritable(average));
		}

		protected void cleanup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
		}
    }

    public int run(String[] args) throws Exception 
	{
		JobConf conf = new JobConf(getConf(), exercise2.class);
		conf.setJobName("exercise2");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(FloatWritable.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
			
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		return 0;
    }

    public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new exercise2(), args);
		System.exit(res);
    }
}


