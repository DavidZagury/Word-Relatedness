package Step.Three;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StepThreeMap extends Mapper<LongWritable, Text, Text, Text> {
	/*INPUT:	A C year num_of_[A,C]	num_of_[A]
	**			B * year	num_of_B
	**OUTPUT:	C year		A C year num_of_[A,C] num_of_[A]
	*/
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {	
		String[] splitedLine = value.toString().split("\\s+");
		if (splitedLine[1].equals("*")) {
			context.write(new Text(splitedLine[2]), value);
			context.write(new Text(splitedLine[0] + " " + splitedLine[2]), value);			
		}	else {
				context.write(new Text(splitedLine[1] + " " + splitedLine[2]), value);
		}
	}
}
