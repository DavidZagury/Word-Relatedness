package Step.Four;

public final class Util {
	public static double getPMI(long word1Count, long word2Count, long word1word2C, long totalWords) {
		return Math.log(word1word2C) + Math.log(totalWords) - Math.log(word1Count) - Math.log(word2Count);
	}
	
}
