import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

	public static class MapperInvertedIndex extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] parts = line.split(",");
			System.out.println(parts);
			if (parts.length == 3) {
				String id = parts[0];
				if (id.equals("id")) //header
					return;
				String domain = parts[1];
				context.write(new Text(domain), new Text(id));
			}
		}
	}
	

	public static class ReducerInvertedIndex extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<String> values, Context context)
				throws IOException, InterruptedException {

			StringBuilder sb = new StringBuilder();
			for (String id : values) {
				sb.append(id.toString() + " ");
			}

			result.set(new Text(sb.substring(0, sb.length() - 1).toString()));
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("yarn.application.classpath",
				"{{HADOOP_CONF_DIR}},{{HADOOP_COMMON_HOME}}/share/hadoop/common/*,{{HADOOP_COMMON_HOME}}/share/hadoop/common/lib/*,"
						+ " {{HADOOP_HDFS_HOME}}/share/hadoop/hdfs/*,{{HADOOP_HDFS_HOME}}/share/hadoop/hdfs/lib/*,"
						+ "{{HADOOP_MAPRED_HOME}}/share/hadoop/mapreduce/*,{{HADOOP_MAPRED_HOME}}/share/hadoop/mapreduce/lib/*,"
						+ "{{HADOOP_YARN_HOME}}/share/hadoop/yarn/*,{{HADOOP_YARN_HOME}}/share/hadoop/yarn/lib/*");

		Job job = Job.getInstance(conf, "Inverted Index");
		job.setJarByClass(InvertedIndex.class);
		job.setMapperClass(MapperInvertedIndex.class);
		job.setCombinerClass(ReducerInvertedIndex.class);
		job.setReducerClass(ReducerInvertedIndex.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}