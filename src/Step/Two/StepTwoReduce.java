package Step.Two;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//import Line.Line;

public class StepTwoReduce extends Reducer<Text, Text, Text, Text>{
	/*INPUT:	A year		A C year num_of_[A,C]
	**			B * year	num_of_B
	**OUTPUT:	A C year num_of_[A,C]	num_of_[A]
	*/
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		long count = 0;
		
		ArrayList<Text> cache = new ArrayList<Text>();

		
		for (Text t : values){
			String[] splitedLine = t.toString().split("\\s+");
			if(splitedLine.length == 4){
				if (splitedLine[1].equals("*")){
					count = Long.parseLong(splitedLine[3]);
					//context.write(t, new Text(Long.toString(count)));
				}
				
			}	
			cache.add(new Text(t));

		}
		
		for (Iterator<Text> iterator = cache.iterator(); iterator.hasNext();){
			Text t = iterator.next();
			context.write(t, new Text(Long.toString(count)));
		}
		
	}

}
