package Step.Four;

import java.util.PriorityQueue;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StepFourReduce extends Reducer<Text, Text, Text, Text> {
	/*
	 * INPUT:	year		A C year num_of_[A,C] num_of_[A] num_of_[C]
	 *			* year		B num_of_B
	 * OUTPUT:	A C year	pmi
	 */	/*	
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		ArrayList<Text> cache = new ArrayList<Text>();
		
		long N = 0;
		for (Text t : values){			
			String[] splitedLine = t.toString().split("\\s+");
			if(splitedLine.length == 1){			
				N = Long.parseLong(splitedLine[0]);	
			}	else {
				cache.add(new Text(t));
			}
		}

		for (Text element : cache){
			String[] line = element.toString().split("\\s+");
			context.write(new Text(line[0] + " " + line[1]+ " " + line[2]) , 
					new Text(Double.toString(Util.getPMI(Long.parseLong(line[4])
							, Long.parseLong(line[5])
							, Long.parseLong(line[3])
							, N))));
		}
	
	}
	
	
	*/
	///////////////////////////////////////////////////////////////
	
	/*
	 * INPUT:	year		A C year num_of_[A,C] num_of_[A] num_of_[C]
	 *			* year	B num_of_B
	**OUTPUT:	A C year		pmi
	*/		

	private int k;
	PriorityQueue<Result> pq;
	
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		k = Integer.parseInt(conf.get("k"));
		pq = new PriorityQueue<Result>(k);	
	}
	
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


		ArrayList<Result> cache = new ArrayList<Result>();
		
		long N = 0;
		for (Text t : values){		
			String[] splitedLine = t.toString().split("\\s+");
			if(splitedLine.length == 1){
				N = Long.parseLong(splitedLine[0]);	
			}	else {
				cache.add(new Result(splitedLine[0], splitedLine[1], splitedLine[2], splitedLine[3], splitedLine[4], splitedLine[5]));
			}
		}

		for (Result element : cache){
			element.setPMI(N);
			pq.add(element);
			if (pq.size() > k){
				pq.remove();
			}
		}
	
		System.out.println("SIZE OF PQ: " + pq.size());
		while(!pq.isEmpty()) {
			Result r = pq.poll();
			context.write(new Text(r.toString()), new Text());
		}
	}
	
	///////////////////////////////////////////////////////////////
	
}
