package Step.Four;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StepFourMap extends Mapper<LongWritable, Text, Text, Text> {
	/*
	 * INPUT:	A C year num_of_[A,C] num_of_[A]	num_of_[C]
	 * 			B * year	num_of_B
	 * OUTPUT:	year		A C year num_of_[A,C] num_of_[A] num_of_[C]
	 */
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {	
		String[] splitedLine = value.toString().split("\\s+");
		if (splitedLine.length == 2) {
			context.write(new Text(splitedLine[0]), new Text(splitedLine[1]));		
		}	else {
				context.write(new Text(splitedLine[2]), value);
		}
	}
}
