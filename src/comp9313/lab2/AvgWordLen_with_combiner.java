

package comp9313.lab2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*
 * Author: Huijun Wu (z5055605)
 * Date: 16th August, 2016
 * Assignment 1 for COMP 9313
 * this class is the one with combiner
 */

public class AvgWordLen_with_combiner {
	
	/*
	 * In this scenario, we cannot just use the reducer as a combiner. Therefore, we design a class called MyArrayWritable. 
	 * The class has two elements, word_length and word_count. So the input of the combiner is the same as the input for reducer
	 * which is Text, MyArrayWritable, and the output is the same. 
	 */
	
	public static class MyArrayWritable extends ArrayWritable{
		
		private DoubleWritable word_length = new DoubleWritable();
		
		private DoubleWritable word_count = new DoubleWritable();
		
		public MyArrayWritable(){
			
			super(DoubleWritable.class);
		}
		
		public MyArrayWritable(double[] data)
		{
			super(DoubleWritable.class);
			
			DoubleWritable[] array = new DoubleWritable[data.length];

			array[0] = new DoubleWritable(data[0]);
			array[1] = new DoubleWritable(data[1]);
			this.word_length = new DoubleWritable(data[0]);
			this.word_count = new DoubleWritable(data[1]);
			set(array);
			
		}
		
		//We have to override the following two functions to implement the serialization and de-serialization of our defined MyWritable type.  
		
		@Override
		public void readFields(DataInput in) throws IOException
		{
			this.word_length.readFields(in);
			this.word_count.readFields(in);
		}
		
		@Override
		public void write(DataOutput dataOutput) throws IOException
		{
			this.word_length.write(dataOutput);
			this.word_count.write(dataOutput);
		}
		
		public DoubleWritable getWordLength()
		{
			return this.word_length;
		}
		
		public DoubleWritable getWordCount()
		{
			return this.word_count;
		}
		
	}
	
	

	public static class TokenizerMapper extends Mapper<Object, Text, Text, MyArrayWritable> {

		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(),
					" 0123456789*$&#/\t\n\f\"'\\,.:;?![](){}<>~-_%@|");

			while (itr.hasMoreTokens()) {

				String original_word = itr.nextToken().toLowerCase();

				word.set(original_word.substring(0, 1));
				
				double[] out_double = {(double)original_word.length(), 1.0};
				
				MyArrayWritable out = new MyArrayWritable(out_double);
				//the output of mapper is just (the first char of the string, MyArrayWritable(the length of the string, one)). 
				context.write(word, out);
			}
		}
	}

	public static class AvgLengthCombiner extends Reducer<Text, MyArrayWritable, Text, MyArrayWritable> {


		public void reduce(Text key, Iterable<MyArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			int count = 0;
			for (MyArrayWritable val : values) {
				
				Writable[] tmp_array = val.get();
				sum += val.getWordLength().get()*val.getWordCount().get();
				count += val.getWordCount().get();
			}
			/*
			 * the combiner combines the <char, MyWritable> pairs with the same starting char. and the output of combiner is 
			 * just (the first char of the stings,MyArrayWritable(the sum of the strings, the number of the strings being 
			 * used to calculate the sum.) )
			 */
			double[] array_out = {(double)(sum),(double)count};
			context.write(key, new MyArrayWritable(array_out));
		}
	}

	public static class AvgLengthReducer extends Reducer<Text, MyArrayWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		public void reduce(Text key, Iterable<MyArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			int count = 0;
			for (MyArrayWritable val : values) {

				Writable[] tmp_array = val.get();
				sum += val.getWordLength().get();
				count += val.getWordCount().get();
			}
			//the reducer just outputs (key = the first char of the strings, the average length of the strings starting with key)
			result.set(sum / count);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(AvgWordLen_with_combiner.class);
		job.setMapOutputValueClass(MyArrayWritable.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(AvgLengthCombiner.class);
		job.setReducerClass(AvgLengthReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}