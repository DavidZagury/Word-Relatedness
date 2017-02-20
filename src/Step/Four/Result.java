package Step.Four;

public class Result implements Comparable<Result>{
	private String a;
	private String b;
	private String aCount;
	private String bCount;
	private String abCount;
	private String year;
	private String pmi;
	
	public Result(String a, String b, String year, String abCount, String aCount, String bCount) {
		this.a = a;
		this.b = b;
		this.abCount = abCount;
		this.year = year;
		this.aCount = aCount;
		this.bCount = bCount;
	}
	
	public String toString() {
		return a + " " + b + " " + year + " " + pmi;
	}
	public void setPMI(long n){
		pmi = Double.toString(Util.getPMI(Long.parseLong(aCount)
				, Long.parseLong(bCount)
				, Long.parseLong(abCount)
				, n));
	}
	@Override
	public int compareTo(Result text2) {
		String[] splitedLine1 = this.toString().split("\\s+");
		String[] splitedLine2 = text2.toString().split("\\s+");
		
		if (Double.parseDouble(splitedLine1[3]) - Double.parseDouble(splitedLine2[3]) > 0)
			return 1;
		else if ((Double.parseDouble(splitedLine1[3]) - Double.parseDouble(splitedLine2[3]) == 0))
			return 0;
		else
			return -1;			
	}

}