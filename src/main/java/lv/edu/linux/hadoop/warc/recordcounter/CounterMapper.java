package lv.edu.linux.hadoop.warc.recordcounter;

import java.io.IOException;
import edu.umd.cloud9.collection.clue.ClueWarcRecord;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;


class CounterMapper extends MapReduceBase implements Mapper<Writable, ClueWarcRecord, Text, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	private Text content_type = new Text();
	
	private static Pattern pattern = Pattern.compile("content-type:\\s*(.*)", Pattern.CASE_INSENSITIVE);

	@Override
	public void map(Writable key, ClueWarcRecord doc, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		
		// Check content type
		if(doc.getHeaderRecordType().equals("response")) {
			byte[] byteContent = doc.getByteContent();
			
			// @TODO doubles used memory :(
			String content = new String(byteContent);
			Matcher m = pattern.matcher(content);
			if (m.find()) {
				content_type.set(m.group(1));
				output.collect(content_type, one);
			}
			else {
				content_type.set("Unknown content type");
				output.collect(content_type, one);
			}
			
		}
		else {
			content_type.set(doc.getHeaderRecordType());
			output.collect(content_type, one);
		}
	}
}
