package lab6;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Uses HBase's bulk load facility ({@link HFileOutputFormat2} and
 * {@link LoadIncrementalHFiles}) to efficiently load data into a
 * HBase table.
 */

public class HBaseBulkLoadExample extends Configured implements Tool {

	//Only the mapper is required, HBase will implement the reducer
	static class HBaseVoteMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

		//in the map function, you parse the source file, and create a put object.
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			Vote vote = new Vote(value.toString());
			String rowKey = vote.getId();

			System.out.println(vote.toString());

			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes("postInfo"), Bytes.toBytes("pid"), Bytes.toBytes(vote.getPostId()));
			put.addColumn(Bytes.toBytes("voteInfo"), Bytes.toBytes("vtype"), Bytes.toBytes(vote.getVoteTypeId()));
			if (vote.getVoteTypeId().equals("5")) {
				put.addColumn(Bytes.toBytes("voteInfo"), Bytes.toBytes("uid"), Bytes.toBytes(vote.getUserId()));
			}
			put.addColumn(Bytes.toBytes("voteInfo"), Bytes.toBytes("cTime"), Bytes.toBytes(vote.getCreationDate()));
			context.write(new ImmutableBytesWritable(Bytes.toBytes(rowKey)), put);
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.fs.tmp.dir", "/tmp/hbase-staging");
		conf.set("hbase.bulkload.staging.dir", "/tmp/hbase-staging");
		Job job = Job.getInstance(conf, "hbase bulk load");

		job.setJarByClass(getClass());
		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/comp9313/Votes"));
		Path outputPath = new Path("hdfs://localhost:9000/user/comp9313/output");
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setMapperClass(HBaseVoteMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);

		Connection connection = ConnectionFactory.createConnection(conf);
		TableName tableName = TableName.valueOf("votes");
		Table table = connection.getTable(tableName);

		try {

			RegionLocator locator = connection.getRegionLocator(tableName);
			HFileOutputFormat2.configureIncrementalLoad(job, table, locator);

			if (!job.waitForCompletion(true)) {
				return 1;
			}

			LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
			Admin admin = connection.getAdmin();
			loader.doBulkLoad(outputPath, admin, table, locator);
			return 0;
		} finally {
			table.close();
		}
	}

	public static void createTable(String tName, String [] CF) {
		Configuration conf = HBaseConfiguration.create();		
		try {
			Connection connection = ConnectionFactory.createConnection(conf);

			try {
				Admin admin = connection.getAdmin();
				TableName tableName = TableName.valueOf(tName);
				if(admin.tableExists(tableName)){
					return;
				}
				try {					
					HTableDescriptor htd = new HTableDescriptor(tableName);

					HColumnDescriptor[] column = new HColumnDescriptor[CF.length];
					for(int i=0;i<CF.length;i++){
						column[i] = new HColumnDescriptor(CF[i]);
						htd.addFamily(column[i]);
					}
					
					admin.createTable(htd);
				} finally {
					admin.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				connection.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		
		createTable("votes", new String[]{"postInfo", "voteInfo"});
		int exitCode = ToolRunner.run(HBaseConfiguration.create(), new HBaseBulkLoadExample(), args);
		System.exit(exitCode);
	}
}