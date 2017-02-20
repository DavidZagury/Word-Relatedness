package Step.One;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StepOneReduce extends Reducer<Text, Text, Text, Text>{
	/*
	**INPUT:	A C year	num
	**			B * year	num
	**OUTPUT:	A C year	num_of_[A,C]
	**			B * year	num_of_[B]
	*/
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		 long count = 0;
		 for (Iterator<Text> iterator = values.iterator(); iterator.hasNext();){
			 count += Long.parseLong(iterator.next().toString());
		 }
			context.write(key, new Text(Long.toString(count)));
	}
}
