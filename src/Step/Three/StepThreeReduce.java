package Step.Three;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StepThreeReduce extends Reducer<Text, Text, Text, Text> {
	/*
	 * INPUT:	C year		A C year num_of_[A,C] num_of_[A]
	 * 			B * year	num_of_B
	 * OUTPUT:	A C year num_of_[A,C] num_of_[A]	num_of_[C]
	*/
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		ArrayList<Text> cache = new ArrayList<Text>();
		
		if (key.toString().length() > 3) {
			long count = 0;
			for (Text t : values){			
				String[] splitedLine = t.toString().split("\\s+");
				if(splitedLine.length == 5){
					if (splitedLine[1].equals("*"))					
						count = Long.parseLong(splitedLine[3]);
					else 
						cache.add(new Text(t));
					
				}	
			}

			for (Text element : cache){
				context.write(element , new Text(Long.toString(count)));
			}
		
		} else {
			long count = 0;
			for (Iterator<Text> iterator = values.iterator(); iterator.hasNext();){
				Text t = iterator.next();
				String[] line = t.toString().split("\\s+");
				count += Long.parseLong(line[3]);
			}
			context.write(key, new Text(Long.toString(count)));
		} 
		
	}
}
