package comp9313.lab2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CoTermNSPair3 {
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text , MapWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();		
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			
			
			while (itr.hasMoreTokens()) {
				
				IdentityHashMap<String, MapWritable> stripes_in_line = new IdentityHashMap<String, MapWritable>();
				
				
				
				String line = itr.nextToken().toLowerCase();
				
				StringTokenizer itr_line = new StringTokenizer(line.toString(), " *$&#/\t\f\"'\\,.:;?![](){}<>~-_");

				
				while(itr_line.hasMoreTokens()){
					
					String cur_str = itr_line.nextToken().toLowerCase().toString();
	
					for(Entry<String, MapWritable> entry:stripes_in_line.entrySet())
					{
						
						if((entry.getValue().get(new Text(cur_str)))!=null)
						{
							IntWritable t = (IntWritable)entry.getValue().get(new Text(cur_str));
							
							IntWritable tmp_int = new IntWritable();
							
							tmp_int.set(t.get() + 1);
							
							entry.getValue().put(new Text(cur_str), tmp_int);
						}
						else
						{
							entry.getValue().put(new Text(cur_str), one);
						}
						
					}
					
					stripes_in_line.put(cur_str, new MapWritable());
					
				}
				
				for(Entry<String, MapWritable> entry:stripes_in_line.entrySet())
				{
					
					
		
					context.write(new Text(entry.getKey().toLowerCase().toString()), entry.getValue());
					
					
					
				}
		

				
			}
		}		
	}
	
	public static class CoTermCombiner extends Reducer<Text, MapWritable, Text, MapWritable>{
		
		private MapWritable result = new MapWritable();
		
		public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException
		{
			HashMap<String,Integer> non_replicated_in_stripe = new HashMap<String, Integer>();
			
			for(MapWritable val : values){
				
				Set<Entry<Writable, Writable>> sets = val.entrySet();
				
				for(Entry<Writable, Writable> entry: sets){
					
					if(non_replicated_in_stripe.get(entry.getKey())==null){
						non_replicated_in_stripe.put(entry.getKey().toString(), Integer.parseInt(entry.getValue().toString()));
					}
					else
					{
						Integer exist_occur = non_replicated_in_stripe.get(entry.getKey().toString());
						non_replicated_in_stripe.put(entry.getKey().toString(), exist_occur + 1);
					}
					
				}
				
				Iterator it = non_replicated_in_stripe.entrySet().iterator();
				
				for(Entry<String, Integer> entry: non_replicated_in_stripe.entrySet())
				{
					
						
						String ret_key = entry.getKey().toString();  
						result.put(new Text(ret_key), new IntWritable(entry.getValue().intValue()));
						context.write(new Text(key), result);
					
				}
				
			}
			
		}
		
	}
	

	public static class IntSumReducer extends Reducer<Text, MapWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {			
			
			HashMap<String,Integer> co_occur = new HashMap<String, Integer>();
			
			for (MapWritable val : values) {
				
				Set<Entry<Writable, Writable>> sets = val.entrySet();
				
				for(Entry<Writable, Writable> entry: sets){
					
					if(co_occur.get(entry.getKey().toString())==null){
						co_occur.put(entry.getKey().toString(), 1);
					}
					else
					{
						Integer exist_occur = co_occur.get(entry.getKey().toString());
						co_occur.put(entry.getKey().toString(), exist_occur + 1);
					}
				}
			}
			Iterator it = co_occur.entrySet().iterator();
			for(Entry<String, Integer> entry: co_occur.entrySet())
			{
				
					
					String ret_key = key.toString() + " " + entry.getKey().toString();  
					result.set(entry.getValue().intValue());
					context.write(new Text(ret_key), result);
				
			}
		
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(CoTermNSPair3.class);
		job.setMapOutputValueClass(MapWritable.class);
		//job.setCombinerClass(CoTermCombiner.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
