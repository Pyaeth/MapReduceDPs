import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCounter {

	public static class MapWC extends Mapper<Object, Text, Text, IntWritable> {
		private Text word = new Text();
		private final static IntWritable one = new IntWritable(1);

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] parts = line.split(",");
				String regex = "!|\\?|:|;|\\\"|`|\\(|\\)|\\{|\\}|”|“|\\d|\\.|('s)|[^a-zA-Z\\s]";
			if (parts.length == 3) {
				String content = parts[2].replaceAll(regex, "")
						.toLowerCase();
				StringTokenizer tokenizer = new StringTokenizer(content);
				while (tokenizer.hasMoreTokens()) {
					word.set(tokenizer.nextToken());
					context.write(word, one);
				}
			}
		}
	}

	public static class ReduceWC extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : values) {
				sum += count.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("yarn.application.classpath",
				"{{HADOOP_CONF_DIR}},{{HADOOP_COMMON_HOME}}/share/hadoop/common/*,{{HADOOP_COMMON_HOME}}/share/hadoop/common/lib/*,"
						+ " {{HADOOP_HDFS_HOME}}/share/hadoop/hdfs/*,{{HADOOP_HDFS_HOME}}/share/hadoop/hdfs/lib/*,"
						+ "{{HADOOP_MAPRED_HOME}}/share/hadoop/mapreduce/*,{{HADOOP_MAPRED_HOME}}/share/hadoop/mapreduce/lib/*,"
						+ "{{HADOOP_YARN_HOME}}/share/hadoop/yarn/*,{{HADOOP_YARN_HOME}}/share/hadoop/yarn/lib/*");

		Job job = Job.getInstance(conf, "Word Counter");
		job.setJarByClass(WordCounter.class);
		job.setMapperClass(MapWC.class);
		job.setCombinerClass(ReduceWC.class);
		job.setReducerClass(ReduceWC.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}