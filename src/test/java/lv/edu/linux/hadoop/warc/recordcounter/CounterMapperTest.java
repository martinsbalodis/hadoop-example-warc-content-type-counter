package lv.edu.linux.hadoop.warc.recordcounter;

import edu.umd.cloud9.collection.clue.ClueWarcRecord;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.ReduceDriver;

import org.junit.Before;
import org.junit.Test;

public class CounterMapperTest {

	private MapDriver<Writable, ClueWarcRecord, Text, IntWritable> mapDriver;
	private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

	@Before
	public void setUp() {
		CounterMapper mapper = new CounterMapper();
		CounterReducer reducer = new CounterReducer();

		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	public void testMapper() {
		ClueWarcRecord rec = new ClueWarcRecord();
		rec.setWarcRecordType("response");
		rec.setContent("cache-control:private, max-age=0\n"
				+ "content-encoding:gzip\n"
				+ "content-type:text/html; charset=UTF-8\n"
				+ "date:Wed, 07 Aug 2013 11:39:59 GMT\n"
				+ "expires:-1\n"
				+ "server:gws\n"
				+ "status:200 OK\n"
				+ "version:HTTP/1.1\n"
				+ "x-frame-options:SAMEORIGIN\n"
				+ "x-xss-protection:1; mode=block\n"
				+ "\n"
				+ "\n"
				+ "<html>test</html>");

		mapDriver.withInput(new Text("key"), rec);
		mapDriver.withOutput(new Text("text/html; charset=UTF-8"), new IntWritable(1));
		mapDriver.runTest();
	}
}