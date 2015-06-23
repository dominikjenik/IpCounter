package hadoop;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Dominik Jenik
 */
public class IpMapper extends Mapper<Object, Text, Text, LongWritable> {

	/**
	 * Parse input into MapWritable data element.
	 */
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// System.out.println("mapper");
		Pattern p = Pattern
				.compile("^([0-9\\.]+)\\s+\\S+\\s+\\S+\\s+\\[[^\\]]+\\]\\s+\\\"[^\\\"]+\\\"\\s+(\\d+)\\s+(\\d+).*$");
		Matcher m = p.matcher(value.toString());
		if (m.find()) {
//			String ip = m.group(1);
			String ip = m.group(2);
			Long bytes = Long.parseLong(m.group(3));
			// System.out.println(ip + " : " + bytes);
			context.write(new Text(ip), new LongWritable(bytes));
		}

	}

}
