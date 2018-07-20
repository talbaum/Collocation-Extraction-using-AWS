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

	@Override
	public int compareTo(Bigram other) {
		double like1=0,like2=0;
		if(likehood.toString().equals("") || likehood.toString().equals("~") ||likehood.toString().equals("NaN"))
		{
			like1=Integer.MIN_VALUE;
		}
		else {
			like1=Double.parseDouble(likehood.toString());
		}
		if(other.getLikehood().toString().equals("") || other.getLikehood().toString().equals("~")||other.getLikehood().toString().equals("NaN"))
		{
			like2=Integer.MIN_VALUE;
		}
		else {
			like2=Double.parseDouble(other.getLikehood().toString());
		}
		 
		
		
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
				if(first.compareTo(other.getFirst()) < 0) {
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

	private boolean isNan(Text likehood) {
		return likehood.toString().equals("NaN");
	}
}


