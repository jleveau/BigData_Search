
package bigdata;


import org.apache.hadoop.util.ProgramDriver;

import pivots.PivotsCached;

public class Main {
	
	public static void main( String[] args ) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;

		try {
		 	//Params : input output nb_cluster column
			pgd.addClass("kmeans1D", kmeans1D.Kmeans1D.class, "Kmean with 1 dimension");
		    // Params : input output nb_cluster [columns]
		 	pgd.addClass("kmeansnD", kmeansnD.KmeansnD.class, "Kmean with n dimensions");
		 	// Params : input output nb_cluster depth [columns]
		 	pgd.addClass("kmeansnDHierar", kmeansnDHierar.KmeansnDHierar.class, "recursive Kmeans with depth h on n dimensions");
		 	// Params : input output label_col measure_col [classification_columns] 
		 	pgd.addClass("labelize", labels.Labelizer.class, "create a label file for clusters");


		//	pgd.addClass("kmeans", kmeans.FindNewPivots.class, "compute pivots from subsets of data");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}

