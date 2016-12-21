package kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;


public class Kmeans1D extends Configured implements Tool {
	public int run(String[] args) throws Exception, ClassNotFoundException, InterruptedException {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "Kmeans");
		
		job.setJarByClass(Kmeans.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setNumReduceTasks(1);
		job.addCacheFile(new URI("/pivots"));
		try {
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			conf.setInt("points.pivot_number", Integer.parseInt(args[2]));
			conf.setInt("column_number", Integer.parseInt(args[3]));
		} 
		catch (Exception e) {
			System.out.println(" bad arguments, waiting for 3 arguments [inputURI] [outputURI][nb_pivot][num_column]");
			return -1;
		}
		
	//	job.setMapperClass(TopKMapper.class);
	//	job.setMapOutputKeyClass(IntWritable.class);
	//	job.setMapOutputValueClass(Text.class);
	//	job.setCombinerClass(TopKCombiner.class);
	//	job.setReducerClass(TopKReducer.class);
		return job.waitForCompletion(true) ? 0 : 1;
	} 
	
	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new Kmeans(), args));
	}
}
