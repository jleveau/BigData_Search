package kmeansnDHierar;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import kmeansnD.KmeansnD;
import kmeansnD.KmeansnDCombinedWritable;
import kmeansnD.KmeansnDDataWritable;
import kmeansnD.KmeansnDCombiner;
import kmeansnD.KmeansnDMapper;
import kmeansnD.kmeansnDReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KmeansnDHierar extends Configured implements Tool {

	int k;
	int depth;
	private ArrayList<Path> cluster_out_paths;

	public int run(String[] args) throws Exception, ClassNotFoundException,
			InterruptedException {

		Configuration conf = getConf();
		Path input_path;
		Path output_path;
		cluster_out_paths = new ArrayList<Path>();

		try {
			input_path = new Path(args[0]);
			output_path = new Path(args[1]);
			k = Integer.parseInt(args[2]);
			depth = Integer.parseInt(args[3]);
			conf.setInt("pivots.number", k);
			conf.setInt("hierarchie.depth", depth);
			int i;
			for (i = 0; i < args.length - 4; ++i) {
				conf.setInt("pivots.column_number." + i,
						Integer.parseInt(args[i + 4]));
			}
			conf.setInt("pivots.dimension", i);
		} catch (Exception e) {
			System.err
					.println(" bad arguments, [inputURI] [outputURI][nb_pivot][depth][num_columns ...]");
			return -1;
		}

		FileSystem fs = FileSystem.get(getConf());

		// working directory
		fs.delete(output_path, true);
		fs.mkdirs(output_path);

		kmeansHierar(input_path, output_path, input_path, conf, depth, 0, "");

		mergefiles(cluster_out_paths, new Path(output_path, "result"));
		// joinColumns(cluster_out_paths, new Path(output_path,"result"));
		return 0;
	}

	private void kmeansHierar(Path input_path, Path output_path,
			Path src_pivot_path, Configuration conf, int depth_acc, int k_acc,
			String list_k) throws IOException, URISyntaxException,
			IllegalArgumentException, ClassNotFoundException,
			InterruptedException {

		FileSystem fs = FileSystem.get(getConf());

		if (depth_acc == 0) {
			cluster_out_paths.add(input_path);
			return;
		}

		String pivot_path;
		String base_path = new Path(output_path, "_iterations").toString();

		// create initial pivot file
		pivot_path = new Path(output_path, "starting_pivots").toString();
		if (!createPivots(src_pivot_path.toString(), pivot_path, k)) {
			System.err.println("Not enough line for " + k + "clusters");
			return;
		}

		// Copy input as SequenceFile
		Path new_input_path = new Path(output_path, "input");
		fs.delete(new_input_path, true);
		copyAsSequenceFile(input_path, new_input_path);
		input_path = new_input_path;

		int iteration = 0;

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
			return;
		long before = job.getCounters().findCounter("SUM", "after").getValue();
		long after;

		// Start iterations
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
			pivot_path = new Path(iteration_output_previous.toString(),
					"part-r-00000").toString();

			// Define the pivot uri for mappers
			job.getConfiguration().set("pivots.uri", pivot_path.toString());

			// Start job
			if (!job.waitForCompletion(true))
				return;

			// compute end condition
			after = job.getCounters().findCounter("SUM", "after").getValue();
			if (Math.abs(after - before) < 0.1) {
				hasUpdates = false;
			}
			before = after;
			iteration_output_previous = iteration_output_current;
		}
		// create new files for next clusters
		Path output_for_cluster = new Path(output_path.toString(), "clusters"
				+ list_k + "_" + k_acc);

		createNewClustersFile(input_path, output_for_cluster, new Path(
				iteration_output_previous.toString(), "part-r-00000"), k);

		// Delete sequenceFile
		fs.delete(input_path, true);
		fs.delete(new Path(base_path), true);

		// Start new depth
		for (int i = 0; i < k; ++i) {
			kmeansHierar(
					new Path(output_for_cluster.toString(), Integer.toString(i)),
					output_path,
					
					new Path(output_for_cluster.toString(),Integer.toString(i)+"/part-m-00000")
					,conf, depth_acc - 1, i, list_k + "_" + k_acc);
		}
	}

	private Job updatePivotsJob(String pivot_path, int iteration)
			throws IOException, URISyntaxException {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "iteration_" + iteration);

		job.setJarByClass(KmeansnD.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(KmeansnDCombinedWritable.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(KmeansnDMapper.class);
		job.setCombinerClass(KmeansnDCombiner.class);
		job.setReducerClass(kmeansnDReducer.class);

		//Set to 1 because there too few reduce tasks (setting to 0 use more time)
	
		job.setNumReduceTasks(1);
		job.addCacheFile(new URI(pivot_path));
		return job;
	}

	public void SequenceFileToText(String input_p, String output_p, int k)
			throws URISyntaxException, IOException {

		URI input_uri = new URI(input_p);
		URI output_uri = new URI(output_p);

		Configuration conf = getConf();
		FileSystem hdfs = FileSystem.get(input_uri, conf);

		OutputStream output = hdfs.create(new Path(output_uri));

		SequenceFile.Reader reader = null;
		reader = new SequenceFile.Reader(conf, Reader.file(new Path(input_p)));

		NullWritable null_w = NullWritable.get();
		KmeansnDDataWritable combined_w = new KmeansnDDataWritable();

		int i = 0;
		while (reader.next(null_w, combined_w)) {
			output.write((i + "\t").getBytes());

			for (int j = 0; j < combined_w.getCoordinates().size(); j++) {
				output.write((Double.toString(combined_w.getCoordinates()
						.get(j))).getBytes());
				if (j != combined_w.getCoordinates().size() - 1)
					output.write(",".getBytes());
			}
			output.write("\n".getBytes());
			++i;
		}

		reader.close();
		output.close();
	}

	// return false if there is not enough line
	public boolean createPivots(String input_p, String output_p, int k)
			throws URISyntaxException, IOException {

		URI input_uri = new URI(input_p);
		URI output_uri = new URI(output_p);

		Configuration conf = getConf();
		FileSystem hdfs = FileSystem.get(input_uri, conf);

		BufferedReader reader = new BufferedReader(new InputStreamReader(
				hdfs.open(new Path(input_uri))));

		String line;
		StringBuilder sb = new StringBuilder();
		Set<String> pivot_set = new HashSet<String>();

		while (pivot_set.size() < k && (line = reader.readLine()) != null) {
			try {
				String[] splits = line.split(",");
				sb.setLength(0);
				int nb_col = conf.getInt("pivots.dimension", 0);
				for (int j = 0; j < nb_col; j++) {
					int col = conf.getInt("pivots.column_number." + j, 0);
					// Check if splits[col] is a valid double
					Double.parseDouble(splits[col]);
					sb.append(splits[col]);
					sb.append(",");
				}
				sb.setLength(sb.length() - 1);
				sb.append("\n");
				pivot_set.add(sb.toString());
			} catch (NumberFormatException e) {

			}
		}

		if (pivot_set.size() < k)
			return false;
		OutputStream output = hdfs.create(new Path(output_uri));

		int i = 0;
		for (String s : pivot_set) {
			output.write((i++ + "	" + s).getBytes());
		}

		reader.close();
		output.close();
		return true;
	}

	private void copyAsSequenceFile(Path input, Path output)
			throws IllegalArgumentException, IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		job.setJobName("Convert to SequenceFile");
		job.setJarByClass(KmeansnD.class);

		job.setMapperClass(kmeansnD.MapperCopyToSequenceFile.class);

		job.setNumReduceTasks(0);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(KmeansnDDataWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		TextInputFormat.addInputPath(job, input);
		SequenceFileOutputFormat.setOutputPath(job, output);

		job.waitForCompletion(true);
	}

	private void createNewClustersFile(Path input_path, Path output_path,
			Path pivot_path, int k) throws IOException, URISyntaxException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);

		job.setJarByClass(KmeansnDHierar.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(KmeansnDDataWritable.class);

		job.setMapperClass(kmeansndCreateClustersMapper.class);

		TextInputFormat.addInputPath(job, input_path);
		for (int i = 0; i < k; ++i) {
			MultipleOutputs.addNamedOutput(job, Integer.toString(i),
					SequenceFileOutputFormat.class, NullWritable.class,
					KmeansnDDataWritable.class);
		}
		TextOutputFormat.setOutputPath(job, output_path);

		job.setNumReduceTasks(1);
		job.addCacheFile(new URI(pivot_path.toString()));
		job.getConfiguration().set("pivots.uri", pivot_path.toString());

		job.waitForCompletion(true);
	}

	public void mergefiles(ArrayList<Path> cluster_out_paths, Path output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);

		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(output, true);

		job.setJobName("MergeFiles");
		job.setJarByClass(KmeansnD.class);

		job.setMapperClass(copyMapper.class);

		job.setNumReduceTasks(1);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		for (int i = 0; i < cluster_out_paths.size(); ++i) {
			MultipleInputs.addInputPath(job, cluster_out_paths.get(i),
					TextInputFormat.class);
		}
		SequenceFileOutputFormat.setOutputPath(job, output);

		job.waitForCompletion(true);
	}

	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new KmeansnDHierar(), args));
	}
}
