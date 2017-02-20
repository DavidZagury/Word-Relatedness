package fmeasure;



import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class THsortedPairsReduce extends Reducer<Text, Text, Text, Text>{


	List<String> line;
	Path outputFile = Paths.get("finalTHandFmeasureList.txt");
	double th;
	private Set<String> givenRelatedPairs = new HashSet<String>(Arrays.asList(
			"tiger jaguar","tiger feline",
			"closet clothes","planet sun","hotel reservation","planet constellation","credit card","stock market","psychology psychiatry",			
			"planet moon","planet galaxy","bank money","physics proton","vodka brandy","war troops","Harvard Yale","news report",
			"psychology Freud","money wealth","man woman","FBI investigation","network hardware","nature environment","seafood food","weather forecast", 
			"championship tournament","law lawyer","money dollar","calculation computation","planet star","Jerusalem Israel","vodka gin",
			"money bank","computer software","murder manslaughter","king queen","OPEC oil","Maradona football","mile kilometer","seafood lobster",
			"furnace stove","environment ecology","boy lad","asylum madhouse","street avenue","car automobile","gem jewel","type kind","magician wizard",
			"football soccer","money currency","money cash","coast shore","money cash","dollar buck","journey voyage","midday noon","tiger tiger")); 
	
	
	private Set<String> givenNotRelatedPairs = new HashSet<String>(Arrays.asList(
		"king cabbage","professor cucumber","chord,smile","noon string",
		"rooster voyage","sugar approach","stock jaguar","stock life","monk slave","lad wizard","delay racism","stock CD","drink ear","stock phone","holy sex","production hike",
		"precedent group","stock egg","energy secretary","month hotel","forest graveyard","cup substance","possibility girl","cemetery woodland","glass magician","cup entity",
		"Wednesday news","direction combination","reason hypertension","sign recess","problem airport","cup article","Arafat Jackson","precedent collection","volunteer motto",
		"listing proximity","opera industry","drink,mother","crane implement","line insurance","announcement effort","precedent cognition","media gain","cup artifact",
		"Mars water","peace insurance","viewer serial","president medal","prejudice recognition","drink car","shore woodland",
		"coast forest","century nation","practice institution","governor interview","money operation","delay news","morality importance","announcement production",
		"five month","school center","experience,music","seven series","report gain","music project","cup object","atmosphere landscape","minority peace",
		"peace atmosphere","morality marriage","stock live","population development","architecture century","precedent information","situation isolation",
		"media trading","profit warning","chance credibility","theater history","day summer","development issue"));
	
	
	
	/* 	input: 	key								TAB		value 
				X_PMI_TH		            TAB    	w1 W2 OriginalPMI
*/


/* 	output: key								TAB		value 
	X_PMI_TH 								TAB      The F_measure for this PMI_TH
*/
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		th = Double.parseDouble(conf.get("th"));
	}
	
	public boolean isRelated(String Pair){
		return (givenRelatedPairs.contains(Pair));
	}
	public boolean isNotRelated(String Pair){
		return (givenNotRelatedPairs.contains(Pair));
	}

	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		for (Text t: values) {
			
			System.out.println("KKKKK:" + key.toString() + " VVVVVVV: " + t.toString());
			
			double Pair_PMI = Double.parseDouble(t.toString());


			if(isRelated(key.toString())){				
				if (Pair_PMI>=th){
					context.getCounter(THsorted.counters.TP).increment(1);
				}
				else {
					context.getCounter(THsorted.counters.FN).increment(1);
				}	
			}
			else if(isNotRelated(key.toString())){				
				if (Pair_PMI>=th){
					context.getCounter(THsorted.counters.FP).increment(1);
					
				}
				else {
					context.getCounter(THsorted.counters.TN).increment(1);
				}	
			}
		}

	}
	
}
	
	
	
	
	
	
	

