package Step.Three;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Step.Two.StepTwo;


public class StepThree {

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();

	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(StepTwo.class);
	    job.setMapperClass(StepThreeMap.class);
	    job.setReducerClass(StepThreeReduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setInputFormatClass(TextInputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));


		job.waitForCompletion(true);

	  }

}
