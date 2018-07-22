package com.amazonaws.samples;

import org.apache.hadoop.io.Text;
//TODO: all of this class
public class BigramFinal extends Bigram  {
	
	public BigramFinal(Text first, Text second, Text decade,Text likehood ) {
		super(first,second,decade,likehood);
	}
	
	public BigramFinal(Text first, Text second, Text decade ) {
		super(first,second,decade,new Text(""));
	}

	public BigramFinal() {
		super();
	}

	@Override
	public String toString() {
		return first + " " + second + " " + decade + " " + " " + likehood;
	}


	private double inputCheck(Text txt) {
		if(txt.toString().equals("") || txt.toString().equals("~") ||txt.toString().equals("NaN"))
			return Integer.MIN_VALUE;
		else
			return 0;
	}
	
	@Override
	public int compareTo(Bigram other) {
		double like1=inputCheck(likehood);
		double like2=inputCheck(other.getLikehood());	
		
		if(decade.compareTo(other.getDecade()) > 0) {
			return 1;
		} else if(decade.compareTo(other.getDecade()) < 0) {
			return -1;
		} else {
			if(like1 < like2) {
				return 1;
			} else if(like1 > like2) {
				return -1;
			} else { 
				if(likehood.compareTo(other.getLikehood())<0) {			
					return 1;
				} else if(likehood.compareTo(other.getLikehood())>0) { 
					return -1;
				}
				else {if(first.compareTo(other.getFirst()) < 0) {
					return 1;
				} else if(first.compareTo(other.getFirst()) > 0) {
					return -1;
				} else {
					if(second.compareTo(other.getSecond()) > 0) {
						return 1;
					} else if(second.compareTo(other.getSecond()) < 0) {
						return -1;
					} else {
						return 0;
					}
				}
			}
		}
		}
	}

}


