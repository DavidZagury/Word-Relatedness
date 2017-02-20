package Step.Two;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StepTwoMap extends Mapper<LongWritable, Text, Text, Text> {
	/*INPUT:	A C year	num_of_[A,C]
	**			B * year	num_of_[B]
	**OUTPUT:	A year		A C year num_of_[A,C]
	*/
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {	
		String[] splitedLine = value.toString().split("\\s+");
		System.out.println(splitedLine[0] + " " + splitedLine[2] + '\t' + value);
		context.write(new Text(splitedLine[0] + " " + splitedLine[2]), value);
	}
	
}
