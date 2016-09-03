package comp9313.lab4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertIndex_ArrayWritable {

	public static class StringArrayWritable extends ArrayWritable {

		public StringArrayWritable() {
			super(Text.class);
		}

		public StringArrayWritable(String[] strings) {

			super(Text.class);
			Text[] texts = new Text[strings.length];
			for (int i = 0; i < strings.length; i++) {
				texts[i] = new Text(strings[i]);
			}
			set(texts);
		}
	}

	public static class Pair implements WritableComparable {

		private Text term1;

		private Text term2;

		public Text getTerm1() {
			return term1;
		}

		public Text getTerm2() {
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
		public int compareTo(Object po) {

			Pair p = (Pair) po;

			String thisTerm1 = term1.toString();
			String thatTerm1 = p.term1.toString();
			String thisTerm2 = term2.toString();
			String thatTerm2 = p.term2.toString();
			int ret = thisTerm1.compareTo(thatTerm1);
			if (ret != 0) {
				// System.out.printf("[1]term1 %s %d term2 %s\n",
				// thisTerm1.toString(),thisTerm1.compareTo(thatTerm1),thatTerm1.toString());
				return thisTerm1.compareTo(thatTerm1);
			}
			// System.out.printf("[2]term1 %s %d term2 %s\n",
			// thisTerm2,thisTerm2.compareTo(thatTerm2), thatTerm2.toString());

			return thisTerm2.compareTo(thatTerm2);

		}

	}

	public static class InvertIndexMapper extends Mapper<Object, Text, Text, StringArrayWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private NullWritable nullValue = NullWritable.get();
		private HashMap<String, Set<String>> in_mapper_table = new HashMap<String, Set<String>>();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");

			ArrayList<String> termArray = new ArrayList<String>();
			while (itr.hasMoreTokens()) {
				String tmp = itr.nextToken().toLowerCase();
				String cur_file = ((FileSplit) context.getInputSplit()).getPath().getName();
				if (in_mapper_table.get(tmp) != null) {

					Set<String> tmp_set = in_mapper_table.get(tmp);

					if (!tmp_set.contains(cur_file)) {
						tmp_set.add(cur_file);
						in_mapper_table.put(tmp, tmp_set);
					}

				}
				else
				{
					Set<String> tmp_set1 = new HashSet<String>();
					tmp_set1.add(cur_file);
					in_mapper_table.put(tmp, tmp_set1);
				}

			}

		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			
			Iterator<Entry<String, Set<String>>> it = in_mapper_table.entrySet().iterator();
			while(it.hasNext()){
				
				Entry<String, Set<String>> tmp_entry = it.next();
				String output_key = tmp_entry.getKey();
				
				Set<String> output_value_set = tmp_entry.getValue();
				
				int set_size = output_value_set.size();
				
				String[] tmp_str = new String[set_size];
				
				int count = 0;
				for(String i : output_value_set){
					tmp_str[count++] = i;
				}
				
				StringArrayWritable out_array = new StringArrayWritable(tmp_str);
		
				
				context.write(new Text(output_key), out_array);
				
			}

		}
	}

	public static class MyPartition extends Partitioner<Pair, IntWritable> {

		@Override
		public int getPartition(Pair key, IntWritable value, int numReduceTasks) {

			return (key.getTerm1().hashCode()) % numReduceTasks;

		}

	}

	public static class InvertIndexReducer extends Reducer<Text, StringArrayWritable, Text, Text> {
		private IntWritable result = new IntWritable();
		private DoubleWritable totalCount = new DoubleWritable();
		private DoubleWritable relativeCount = new DoubleWritable();
		private HashMap<String, Integer> tmp = new HashMap<String, Integer>();
		// private Text flag = new Text("*");

		private Text CurTerm = new Text("NOT_SET");

		private String docList = new String();


		public void reduce(Text key, Iterable<StringArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			
			String final_output = new String();
			
			Set<String> docSet = new HashSet<String>();

			ArrayList<String> out = new ArrayList<String>();
			
			

				
				for(StringArrayWritable i : values){

					String[] strings = i.toStrings();
					int str_len = strings.length;
					
					for(int q = 0; q < str_len; q++){
						
						docSet.add(strings[q]);
					}
				}
			
				
			
				for(String s: docSet)
				{
					out.add(s);
				}
				
				Collections.sort(out);
				
				int array_len = out.size();
				
				
				
				for(int index = 0; index < array_len; index++){
					
					final_output = final_output + out.get(index);
				}
				
				context.write(key, new Text(final_output));
				

			
		}

		private int getTotalCount(Iterable<IntWritable> values) {
			
			int count = 0;
			for (IntWritable value : values) {

				count += value.get();

			}

			return count;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "invert indexing by using StringArrayWritable");
		job.setJarByClass(InvertIndex_ArrayWritable.class);

		job.setMapperClass(InvertIndexMapper.class);
		// job.setCombinerClass(InvertIndexReducer.class);
		job.setReducerClass(InvertIndexReducer.class);
		//job.setPartitionerClass(MyPartition.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringArrayWritable.class);
		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/comp9313/input_dir/"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/comp9313/output"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}