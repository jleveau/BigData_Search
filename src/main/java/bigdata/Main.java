
package bigdata;

import kmeans.FindNewPivots;
import kmeans.Kmeans;
import kmeans.SelectFromList;

import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.util.ProgramDriver;

public class Main {
	
	
	public static void main( String[] args ) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;

		try {
			pgd.addClass("select_pivots", kmeans.SelectFromList.class, "select k pivots in a list of data");
		 	pgd.addClass("kmeans1D", kmeans.Kmeans1D.class, "Kmean with 1 dimension");
		//	pgd.addClass("kmeans", kmeans.FindNewPivots.class, "compute pivots from subsets of data");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}