package hadoop;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Dominik Jenik
 */
public class IpCounter {

	/**
	 * Execution of the computation on Apache Hadoop.
	 * 
	 * @param args
	 *            [0] input folder path (e.g.
	 *            file:///home/pds/workspace/site.log) 
	 *            [1] output folder path
	 *            (e.g. file:///home/pds/workspace/out/)
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		FileUtil.fullyDelete(new File("/home/pds/workspace/outip"));
		long time = System.currentTimeMillis();
		System.out.println("running IpCounter");
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ip-counter");
		job.setJarByClass(IpCounter.class);
		job.setMapperClass(IpMapper.class);
		// job.setCombinerClass(IpCombiner.class);
		job.setReducerClass(IpReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.out.println("running IpCounter");
		boolean completed = job.waitForCompletion(true);
		System.out.println((System.currentTimeMillis() - time) / 1000.0);
		System.exit(completed ? 0 : 1);
	}

}
