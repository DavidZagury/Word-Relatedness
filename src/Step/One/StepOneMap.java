package Step.One;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StepOneMap extends Mapper<LongWritable, Text, Text, Text> {
	/*
	**INPUT:	key 	A B C D E	year num int int
	**OUTPUT:	A C year	num
	**			B C year	num
	**			D C year	num
	**			E C year	num
	**			A * year	num
	**			B * year	num
	**			C * year	num
	**			D * year	num
	**			Y * year	num
	*/
		
	private static final String stopW_Url = "http://web.archive.org/web/20080501010608/http://www.dcs.gla.ac.uk/idom/ir_resources/linguistic_utils/stop_words";	    
	ArrayList<String> stopWords = new ArrayList<>();
	  
	protected void map(LongWritable key, Text value, Context context) {			
		String[] splitedLine = value.toString().split("\t");
		//We only consider decades above 1900	
		if (splitedLine.length > 1) {
			if (Integer.parseInt(splitedLine[1]) >= 1900) { 
				//We take the words part of the given n-gram and split + lowercase it
				String[] lineWords = splitedLine[0].split(" "); 
				//Clean the current word from non-relevant characters
				for(int i = 0; i < lineWords.length; i++) {
					String s = shave(lineWords[i]);
					if (stopWords.contains(s))
						lineWords[i] = "";
					else 					
						lineWords[i] = s;
				}
				//String C = lineWords[lineWords.length/2];
				int middle = lineWords.length/2;
				
				if (lineWords.length > 0 && lineWords[middle].length() > 0){
					for (int i = 0; i < lineWords.length ;i++){
						try {
						if (i != middle && lineWords[i].length() > 0){
	
								if (lineWords[i].compareTo(lineWords[middle]) > 0)	{		
									context.write(new Text(lineWords[i] + " " + lineWords[middle] + " " + splitedLine[1].substring(0, 3)),
										new Text(splitedLine[2]));
									context.write(new Text(lineWords[i] + " * " + splitedLine[1].substring(0, 3)),
											new Text(splitedLine[2]));
								}
								else{
									context.write(new Text(lineWords[middle] + " " + lineWords[i] + " " + splitedLine[1].substring(0, 3)),
										new Text(splitedLine[2]));
									context.write(new Text(lineWords[i] + " * " + splitedLine[1].substring(0, 3)),
											new Text(splitedLine[2]));
									
								}
	
						}
						else {
							context.write(new Text(lineWords[i] + " * " + splitedLine[1].substring(0, 3)),
									new Text(splitedLine[2]));
						}
						} catch (IOException | InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				
			}
		}
		
	}
	
	public String shave(String word){
		return word.toLowerCase().replaceAll("[^a-z]", "");
    }
	
	protected void setup (Context context){
		extractSWORDS();
	}
	
	 private void extractSWORDS() {
	  	try {
          URL webS = new URL(stopW_Url);
          ReadableByteChannel readableBC = Channels.newChannel(webS.openStream());
          FileOutputStream fos = new FileOutputStream("stop_words");
          fos.getChannel().transferFrom(readableBC, 0, Long.MAX_VALUE);
          fos.close();

          // Extract stop Words From File
          BufferedReader buffReader = new BufferedReader(new InputStreamReader(new FileInputStream("stop_words"), "UTF-8"));
          String line;

          while ((line = buffReader.readLine()) != null) {
              stopWords.add(line);
          }
	          buffReader.close();
	      } catch (Exception e) {
	          e.printStackTrace();
	      }		
	}
}
