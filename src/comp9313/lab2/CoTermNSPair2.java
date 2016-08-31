package comp9313.lab2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CoTermNSPair2 {
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();		
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
		
			
			while (itr.hasMoreTokens()) {
				
				String line = itr.nextToken().toLowerCase();
				
				StringTokenizer itr_line = new StringTokenizer(line.toString(), " *$&#/\t\f\"'\\,.:;?![](){}<>~-_");
				
				ArrayList<String> al = new ArrayList<String>();
				
				while(itr_line.hasMoreTokens())
				{
			
					String cur_str = itr_line.nextToken().toLowerCase();
	
					if(!al.isEmpty())
					{
						Iterator it = al.iterator();
						
						
						
						while(it.hasNext())
						{
							
							String ret_pair = new String();
							String former_str = it.next().toString();
							if(former_str.compareTo(cur_str)<=0){
								ret_pair = former_str + " " + cur_str;
							}
							else
							{
								ret_pair = cur_str + " " + former_str;
							}
							word.set(ret_pair);
							context.write(word, one);
					
						}
						al.add(cur_str);
					}
					else
					{
						al.add(cur_str);
						
					}
					

				}
			}
		}		
	}
	
	

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(CoTermNSPair2.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
