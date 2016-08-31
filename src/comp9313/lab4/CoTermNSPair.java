package comp9313.lab4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CoTermNSPair {
	
	
	
	public static class Pair implements WritableComparable {

		private Text term1;

		private Text term2;
		
		public Text getTerm1()
		{
			return term1;
		}
		
		public Text getTerm2()
		{
			return term2;
		}

		public Pair() {

		}

		public Pair(Text t1, Text t2) {

			set(t1, t2);

		}

		public void set(Text t1, Text t2) {
			term1 = t1;
			term2 = t2;
		}

		// We have to override the following two functions to implement the
		// serialization and de-serialization of our defined MyWritable type.

		@Override
		public void readFields(DataInput in) throws IOException {
			String[] strings = WritableUtils.readStringArray(in);
			term1 = new Text(strings[0]);
			term2 = new Text(strings[1]);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			String[] strings = { term1.toString(), term2.toString() };
			WritableUtils.writeStringArray(out, strings);
		}
		@Override
		public int compareTo(Object po){
			
			Pair p = (Pair)po;
			
			
			
			String thisTerm1 = term1.toString();
			String thatTerm1 = p.term1.toString();
			String thisTerm2 = term2.toString();
			String thatTerm2 = p.term2.toString();
			int ret = thisTerm1.compareTo(thatTerm1);
			if(ret!=0)
			{
				//System.out.printf("[1]term1 %s %d term2 %s\n", thisTerm1.toString(),thisTerm1.compareTo(thatTerm1),thatTerm1.toString());
				return thisTerm1.compareTo(thatTerm1);
			}
			//System.out.printf("[2]term1 %s %d term2 %s\n", thisTerm2,thisTerm2.compareTo(thatTerm2), thatTerm2.toString());
			else
			{
				if(thisTerm1.equals("*"))
				{
					return -1;
				}
				else if(thatTerm1.equals("*"))
				{
					return 1;
				}
				
			}
			return thisTerm2.compareTo(thatTerm2);
		
		}
		


	}
	public static class TokenizerMapper extends Mapper<Object, Text, Pair, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();		
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
			
			ArrayList<String> termArray = new ArrayList<String>();
			while (itr.hasMoreTokens()) {
				termArray.add(itr.nextToken().toLowerCase());
			}
			for(int i=0;i<termArray.size();i++){
				String term1 = termArray.get(i);
				for(int j=i+1;j<termArray.size();j++){
					String term2 = termArray.get(j);
					//This is for symmetric computation 
					/*if(term1.compareTo(term2) < 0){
						word.set(term1 + " " + term2);						
					}else{
						word.set(term2 + " " + term1);
					}*/
					
					//This is for nonsymmetric computation
					//word.set(term1 + " " + term2);
					//context.write(word, one);
					//word.set(term1 + " " + "*" );
					//context.write(word, one);
					
					Pair out1 = new Pair(new Text(term1), new Text(term2));
					Pair out2 = new Pair(new Text(term1), new Text("*"));
					context.write(out1, one);
					context.write(out2, one);
					
				}
			}				
		}		
	}

	public static class MyPartition extends Partitioner<Pair, IntWritable>{
		
		@Override
		public int getPartition(Pair key, IntWritable value, int numReduceTasks){

			
			return (key.getTerm1().hashCode())%numReduceTasks;
			
		}
		
		
		
	}
	
	public static class NSPairReducer extends Reducer<Pair, IntWritable, Text, DoubleWritable> {
		private IntWritable result = new IntWritable();
		private DoubleWritable totalCount  = new DoubleWritable();
		private DoubleWritable relativeCount  = new DoubleWritable();
		private HashMap<String, Integer> tmp = new HashMap<String, Integer>();
		private Text flag = new Text("*");
		
		private Text CurKey = new Text("NOT_SET");
		
		

		public void reduce(Pair key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
		
			if(key.getTerm2().equals(flag))
			{
				if(key.getTerm1().equals(CurKey))
				{
					totalCount.set(totalCount.get() + getTotalCount(values) );
				}else{
					
					CurKey.set(key.getTerm1());
					totalCount.set(0);
					totalCount.set(getTotalCount(values));
				}
			}
			else		
			{	
				int count = getTotalCount(values);
				relativeCount.set((double)count/totalCount.get());
				context.write(new Text(key.getTerm1().toString() + "," + key.getTerm2().toString()), relativeCount);
				
				
				
			}
			

		}
		
		private int getTotalCount(Iterable<IntWritable> values){
			
			int count = 0;
			for (IntWritable value:values){
				
				count += value.get();
				
			}
			
			return count;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "term co-occurrence nonsymmetric pair");
		job.setJarByClass(CoTermNSPair.class);
	
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(NSPairReducer.class);
		job.setReducerClass(NSPairReducer.class);
		job.setPartitionerClass(MyPartition.class);
		job.setOutputKeyClass(Pair.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}