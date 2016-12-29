
package bigdata;

import org.apache.hadoop.util.ProgramDriver;

import pivots.PivotsCached;
import pivots.PivotsSequenced;

public class Main {
	
	public static void main( String[] args ) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;

		try {
			pgd.addClass("sequenced", PivotsSequenced.class, "select k pivots in a list of data and store it in a SequencedFile");
			pgd.addClass("pivots", PivotsCached.class, "select k pivots in a list of data and store it in a file");
		 	pgd.addClass("kmeans1D", kmeans1D.Kmeans1D.class, "Kmean with 1 dimension");
		//	pgd.addClass("kmeans", kmeans.FindNewPivots.class, "compute pivots from subsets of data");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}

