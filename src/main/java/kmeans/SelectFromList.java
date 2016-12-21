package kmeans;

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


public class SelectFromList  extends Configured implements Tool {

	public int run(String[] args) throws Exception {
        
       	Configuration conf = getConf();
	    
	
		URI input_uri = new URI(args[0]);
		URI output_uri = new URI("/pivots.txt");
		Integer k = new Integer(args[2]);

		Job job = Job.getInstance(conf, "Kmeans");

		FileSystem hdfs = FileSystem.get(input_uri,conf);

        BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(new Path(input_uri))));
        OutputStream output = hdfs.create(new Path(output_uri));

        String line;
        reader.readLine();
        int i=0;
        
        while (i<k && (line = reader.readLine()) != null){
            i++;
            output.write(line.getBytes());
            output.write("\n".getBytes());
        }
        
        reader.close();
        output.close();

		return 0;
	}
	
	public static void main(String args[]) throws Exception {
		//Take 3 parameters : input output k
		System.exit(ToolRunner.run(new SelectFromList(), args));
	}

}
