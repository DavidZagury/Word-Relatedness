package fmeasure;


import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class THsortedPairsMap extends Mapper<LongWritable, Text, Text, Text> { 
	
	private Set<String> givenPairs;
	
	protected void setup (Context context){
		givenPairs = new HashSet<String>(Arrays.asList(
				"tiger jaguar","tiger feline",
				"closet clothes","planet sun","hotel reservation","planet constellation","credit card","stock market","psychology psychiatry",			
				"planet moon","planet galaxy","bank money","physics proton","vodka brandy","war troops","Harvard Yale","news report",
				"psychology Freud","money wealth","man woman","FBI investigation","network hardware","nature environment","seafood food","weather forecast", 
				"championship tournament","law lawyer","money dollar","calculation computation","planet star","Jerusalem Israel","vodka gin",
				"money bank","computer software","murder manslaughter","king queen","OPEC oil","Maradona football","mile kilometer","seafood lobster",
				"furnace stove","environment ecology","boy lad","asylum madhouse","street avenue","car automobile","gem jewel","type kind","magician wizard",
				"football soccer","money currency","money cash","coast shore","money cash","dollar buck","journey voyage","midday noon","tiger tiger",
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

		
	}
	

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {	
		String[] lineSplit = value.toString().split("\\s+");
		
		System.out.println(value.toString());
		
		if (Integer.parseInt(lineSplit[2])==200){
			context.write(new Text(lineSplit[0] + " " + lineSplit[1]), new Text(lineSplit[3]));
		}
		else{
			return;
		}
	
    }
	
	
	/////////////////////////////////////////////////////////////////////////////////////////
	public boolean isRelated(String Pair){
		return (givenPairs.contains(Pair));
	}
	
}
