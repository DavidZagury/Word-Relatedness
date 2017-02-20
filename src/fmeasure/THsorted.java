package fmeasure;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class THsorted {
	static enum counters { TP, TN, FP, FN }

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    conf.set("th", "0.5");

	    Job job = Job.getInstance(conf, "THsorted");
	    job.setJarByClass(THsorted.class);
	    job.setMapperClass(THsortedPairsMap.class);
	    job.setReducerClass(THsortedPairsReduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setInputFormatClass(TextInputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));


		job.waitForCompletion(true);
   
		
		
		System.out.println("FFFF: " + F_measureCalculate(
				job.getCounters().findCounter(THsorted.counters.TP).getValue(),
				job.getCounters().findCounter(THsorted.counters.FP).getValue(),
				job.getCounters().findCounter(THsorted.counters.FN).getValue()));

	  }

	
	
	
	

	public static double F_measureCalculate(double tp,double fp,double fn){
		if((tp+fn)!=0 && (tp+fp)!=0){
			double recall = tp/(tp+fn);
			double Precision =tp/(tp+fp);
			
			if (recall+Precision!=0){
				return 2*((recall*Precision)/(recall+Precision));
			}
		}
		return 0;
	}
}
