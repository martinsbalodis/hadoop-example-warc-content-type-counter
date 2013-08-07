package lv.edu.linux.hadoop.warc.recordcounter;

import edu.umd.cloud9.collection.clue.ClueWarcInputFormat;
import java.io.File;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;

public class CounterLocalRunner {

	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf();
		conf.setJobName("WarcCounter");

		// This line specifies the jar Hadoop should use to run the mapper and
		// reducer by telling it a class that’s inside it
		conf.setJarByClass(CounterLocalRunner.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(CounterMapper.class);
		conf.setReducerClass(CounterReducer.class);

		// KeyValueTextInputFormat treats each line as an input record,
		// and splits the line by the tab character to separate it into key and value
		conf.setInputFormat(ClueWarcInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);


		// Run this job locally
		conf.set("mapreduce.jobtracker.address", "local");
		conf.set("fs.defaultFS", "file:///");

		String output_dir = "/home/martins/output/";
		deleteLocalDir(new File(output_dir));

		FileInputFormat.setInputPaths(conf, new Path("/home/martins/warcs"));
		FileOutputFormat.setOutputPath(conf, new Path(output_dir));

		JobClient.runJob(conf);
	}

	private static boolean deleteLocalDir(File directory) {
		if (directory.exists()) {
			File[] files = directory.listFiles();
			if (null != files) {
				for (int i = 0; i < files.length; i++) {
					if (files[i].isDirectory()) {
						deleteLocalDir(files[i]);
					} else {
						files[i].delete();
					}
				}
			}
		}
		return (directory.delete());
	}
}
