package hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IpCombiner extends
		Reducer<Text, LongWritable, Text, LongWritable> {

	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		System.out.println("combiner");
		long bytes = 0;
		for (LongWritable byteVal : values) {
			bytes += byteVal.get();
		}
//		System.out.println(key + " C: " + bytes);
		context.write(key, new LongWritable(bytes));
	}
}
