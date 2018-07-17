package com.amazonaws.samples;

import org.apache.hadoop.io.Text;
//TODO: all of this class
public class BigramFinal extends Bigram  {


    public BigramFinal(Text first, Text second, Text decade ) {
        super(first,second,decade);
    }

    public BigramFinal() {
        super();
    }

    @Override
    public String toString() {
        return first + " " + second + " " + decade + " " ;
    }

    @Override
    public int compareTo(Bigram tp) {
        if(decade.compareTo(tp.getDecade()) > 0) {
            return 1;
        } else if(decade.compareTo(tp.getDecade()) < 0) {
            return -1;
        } else {
                    if(first.compareTo(tp.getFirst()) < 0) {
                        return 1;
                    } else if(first.compareTo(tp.getFirst()) > 0) {
                        return -1;
                    } else {
                        if(second.compareTo(tp.getSecond()) > 0) {
                            return 1;
                        } else if(second.compareTo(tp.getSecond()) < 0) {
                            return -1;
                        } else {
                            return 0;
                        }
                    }
                }
        }
    }


