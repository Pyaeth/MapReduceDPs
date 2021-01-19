import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountArticlesPerDomain {

	public static class MapCAPD extends Mapper<Object, Text, NullWritable, NullWritable> {

		public static final String DOMAIN_COUNTER_GROUP = "Domain";
		private HashSet<String> domainsSet = new HashSet<String>();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] parts = line.split(",");
			if (parts.length == 3) {
				String domain = parts[1];
				if (!domainsSet.contains(domain))
					domainsSet.add(domain);
				context.getCounter(DOMAIN_COUNTER_GROUP, domain).increment(1);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("yarn.application.classpath",
				"{{HADOOP_CONF_DIR}},{{HADOOP_COMMON_HOME}}/share/hadoop/common/*,{{HADOOP_COMMON_HOME}}/share/hadoop/common/lib/*,"
						+ " {{HADOOP_HDFS_HOME}}/share/hadoop/hdfs/*,{{HADOOP_HDFS_HOME}}/share/hadoop/hdfs/lib/*,"
						+ "{{HADOOP_MAPRED_HOME}}/share/hadoop/mapreduce/*,{{HADOOP_MAPRED_HOME}}/share/hadoop/mapreduce/lib/*,"
						+ "{{HADOOP_YARN_HOME}}/share/hadoop/yarn/*,{{HADOOP_YARN_HOME}}/share/hadoop/yarn/lib/*");

		Job job = Job.getInstance(conf, "Count Articles per Domain");
		job.setJarByClass(CountArticlesPerDomain.class);
		job.setMapperClass(MapCAPD.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}