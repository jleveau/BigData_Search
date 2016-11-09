
package bigdata;

import kmeans.Kmeans;

import org.apache.hadoop.util.ProgramDriver;

public class Main {
	
	public static void main( String[] args ) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("kmeans", Kmeans.class, "select k pivots in a list of data");
			pgd.addClass("kmeans", Kmeans.class, "compute subsets of data use pivots given as params");
			pgd.addClass("kmeans", Kmeans.class, "compute pivots from subsets of data");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}