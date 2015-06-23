package hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IpReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		// System.out.println("reducer");
		long bytes = 0;
		int count = 0;
		for (LongWritable byteVal : values) {
			bytes += byteVal.get();
			count++;
		}
		// System.out.println(key);
		// if(key.toString().contains("178.154.179.250")){
		// System.out.println("size: "+count);
		// }
		// System.out.println(key + " R: " + bytes);
		context.write(key, new LongWritable(bytes));
	}
}
