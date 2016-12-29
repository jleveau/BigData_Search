package pivots;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class PivotsCached  extends Configured implements Tool {

	public int run(String[] args) throws Exception {
       	Configuration conf = getConf();
	
		URI input_uri = new URI(args[0]);
		URI output_uri = new URI("/resources/pivots");
		Integer k = new Integer(args[1]);
		
		Job job = Job.getInstance(conf, "Kmeans");

		FileSystem hdfs = FileSystem.get(input_uri,conf);

        BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(new Path(input_uri))));
        OutputStream output = hdfs.create(new Path(output_uri));

        String line;
        reader.readLine();
        int i=0;
        
        while (i<k && (line = reader.readLine()) != null){
            String[] splits = line.split(",");
            try {
            	//Check if splits[4] is a valid double
            	Double.parseDouble(splits[4]);
            	output.write(splits[4].getBytes());
            	output.write("\n".getBytes());
            	i++;
            }
            catch(NumberFormatException e){
            	
            }
            
        }
        
        reader.close();
        output.close();

		return 0;
	}

	public static void main(String args[]) throws Exception {
		//Take 3 parameters : input output k
		System.exit(ToolRunner.run(new PivotsCached(), args));
	}

}
