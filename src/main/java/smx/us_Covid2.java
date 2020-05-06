package smx;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class us_Covid2 {

	public static class Map extends
			Mapper<LongWritable, Text, Text, FloatWritable> {
		private final static FloatWritable deadrate = new FloatWritable(1);
		private Text state = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			   String str[]=line.split("\t");
			         if(str.length>3){
			             state.set(str[2]);
			         int i=Integer.parseInt(str[4]);
			         int cases= Integer.parseInt(str[3]);
			         float dr=0;
			         if(cases!=0)
			         {
			        	dr=(float)i/(float)cases;
			         }
			        
			         deadrate.set(dr);
			            
			         
			       }
			          context.write(state, deadrate);
		}
	}

	public static class Reduce extends
			Reducer<Text, FloatWritable, Text, FloatWritable> {

		public void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			double sum = 0;
			int n = 0;
			for (FloatWritable val : values) {
				n=n+1;
				sum += val.get();
			}
			float rate = (float)sum/(float)n;
			context.write(key, new FloatWritable(rate));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "us_Covid");
		job.setJarByClass(us_Covid2.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
