package kmeans1D;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

public class Kmeans1D extends Configured implements Tool {

	public int run(String[] args) throws Exception, ClassNotFoundException,
			InterruptedException {

		Configuration conf = getConf();
		String pivot_path;
		Path input_path;
		Path output_path;
		String base_path;
		try {
			input_path = new Path(args[0]);
			output_path = new Path(args[1]);
			base_path = new Path(args[1], "_iterations").toString();
			conf.setInt("pivots.number", Integer.parseInt(args[2]));
			conf.setInt("pivots.column_number", Integer.parseInt(args[3]));
		} catch (Exception e) {
			System.out
					.println(" bad arguments, waiting for 4 arguments [inputURI] [outputURI][nb_pivot][num_column]");
			return -1;
		}

		FileSystem fs = FileSystem.get(getConf());

		// working directory
		fs.delete(output_path, true);
		fs.mkdirs(output_path);
		
		// create initial pivot file
		pivot_path = new Path(output_path, "starting_pivots").toString();
		createPivots(input_path.toString(), pivot_path, new Integer(args[2]), new Integer(args[3]));
		int iteration = 0;
		
		//Copy input as SequenceFile
		Path new_input_path = new Path(args[1],"input");
		copyAsSequenceFile(input_path,new_input_path);
		input_path = new_input_path;

		// Create first job using default pivots
		Job job = updatePivotsJob(pivot_path, iteration);
		FileInputFormat.addInputPath(job, input_path);
		FileOutputFormat.setOutputPath(job,
				new Path(base_path, Integer.toString(iteration)));
		job.getConfiguration().set("pivots.uri", pivot_path.toString());

		Path iteration_output_previous = new Path(base_path,
				Integer.toString(iteration));
		Path iteration_output_current;
		if (!job.waitForCompletion(true))
			return -1;
		long before = job.getCounters().findCounter("SUM", "after").getValue();
		long after;
		
		boolean hasUpdates = true;
		while (hasUpdates) {
			++iteration;
			
			// create the new job using the output of previous job
			job = updatePivotsJob(pivot_path, iteration);
			FileInputFormat.addInputPath(job, input_path);
			
			// iteration-1 to take the previous result
			iteration_output_current = new Path(base_path,
					Integer.toString(iteration));
			FileOutputFormat.setOutputPath(job, iteration_output_current);
			pivot_path = new Path(iteration_output_previous.toString(),"part-r-00000").toString();
			
			// Define the pivot uri for mappers
			job.getConfiguration().set("pivots.uri", pivot_path.toString());

			// Start job
			if (!job.waitForCompletion(true))
				return -1;

			// compute end condition
			after = job.getCounters().findCounter("SUM", "after").getValue();
			if (Math.abs(after - before) < 1000000) {
				hasUpdates = false;
			}
			before = after;
			iteration_output_previous = iteration_output_current;
		}
		addColumn(input_path,new Path(output_path.toString(),"result") ,
				new Path(iteration_output_previous.toString(),"part-r-00000"));
		return 0;
	}

	private Job updatePivotsJob(String pivot_path, int iteration)
			throws IOException, URISyntaxException {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "iteration_" + iteration);

		job.setJarByClass(Kmeans1D.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Kmeans1DCombinedWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(Kmeans1DMapper.class);
		job.setCombinerClass(Kmeans1DCombiner.class);
		job.setReducerClass(kmeans1DReducer.class);

		job.setNumReduceTasks(1);
		job.addCacheFile(new URI(pivot_path));
		return job;
	}

	public void createPivots(String input_p, String output_p, int k, int col)
			throws URISyntaxException, IOException {

		URI input_uri = new URI(input_p);
		URI output_uri = new URI(output_p);

		Configuration conf = getConf();
		FileSystem hdfs = FileSystem.get(input_uri, conf);

		BufferedReader reader = new BufferedReader(new InputStreamReader(
				hdfs.open(new Path(input_uri))));
		OutputStream output = hdfs.create(new Path(output_uri));

		String line;
		reader.readLine();
		int i = 0;

		while (i < k && (line = reader.readLine()) != null) {
			String[] splits = line.split(",");
			try {
				// Check if splits[] is a valid double
				Double.parseDouble(splits[col]);
				output.write((i + "	" + splits[col]).getBytes());
				output.write("\n".getBytes());
				i++;
			} catch (NumberFormatException e) {
			}
		}

		reader.close();
		output.close();
	}

	private void copyAsSequenceFile(Path input, Path output) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf);
	    job.setJobName("Convert to SequenceFile");
	    job.setJarByClass(Mapper.class);

	    job.setMapperClass(Mapper.class);
	    job.setReducerClass(Reducer.class);

	    job.setNumReduceTasks(0);

	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);

	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    job.setInputFormatClass(TextInputFormat.class);

	    TextInputFormat.addInputPath(job, input);
	    SequenceFileOutputFormat.setOutputPath(job, output);

	    job.waitForCompletion(true);
	}
	
	private void addColumn(Path input_path, Path output_path, Path pivot_path) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException{
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);

		job.setJarByClass(Kmeans1D.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Kmeans1DAddColumnMapper.class);
		//job.setCombinerClass(Kmeans1DCombiner.class);
		//job.setReducerClass(kmeans1DReducer.class);
		
		TextInputFormat.addInputPath(job, input_path);
	    SequenceFileOutputFormat.setOutputPath(job, output_path);

		job.setNumReduceTasks(1);
		job.addCacheFile(new URI(pivot_path.toString()));
		job.getConfiguration().set("pivots.uri", pivot_path.toString());
		
	    job.waitForCompletion(true);
	}
	
	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new Kmeans1D(), args));
	}
}
