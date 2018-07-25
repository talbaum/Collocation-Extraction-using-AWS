package com.amazonaws.samples;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Bigram implements WritableComparable<Bigram> {
    protected Text first;
    protected Text second;
    protected Text decade;
    protected Text likehood;

    public Bigram(Text first, Text second, Text decade , Text likehood) {
    	this.first = first;
        this.second = second;
        this.decade = decade;
        this.likehood = likehood;
    }
    
    public Bigram(Text first, Text second, Text decade) {
    	this.first = first;
        this.second = second;
        this.decade = decade;
        this.likehood = new Text("");
    }

    public Bigram() {
        this.first = new Text();
        this.second = new Text();
        this.decade = new Text();
        this.likehood = new Text();
    }

    public Text getDecade() {
        return decade;
    }

    public void setDecade(Text decade) {
        this.decade = decade;
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }
    
    public Text getLikehood() {
    	return likehood;
    }
    
    public void setLikehood(Text likehood) {
        this.likehood = likehood;
    }
    
    public Text toText() {
        return new Text(this.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
        decade.readFields(in);
        likehood.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
        decade.write(out);
        likehood.write(out);
    }

    @Override
    public String toString() {
        return first + " " + second + " " + decade + " "+likehood;
    }

    @Override
    public int compareTo(Bigram other) {
        if(decade.compareTo(other.getDecade()) > 0) {
            return 1;
        } else if(decade.compareTo(other.getDecade()) < 0) {
            return -1;
        } else {
            if(first.compareTo(other.getFirst()) > 0) {
                return 1;
            } else if(first.compareTo(other.getFirst()) < 0) {
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

    @Override
    public int hashCode() {
                int result = first != null ? first.hashCode() : 0;
                result = 31 * result + (second != null ? second.hashCode() : 0);
                result = 31 * result + (decade != null ? decade.hashCode() : 0);
                result = 31 * result + (likehood != null ? likehood.hashCode() : 0);
                return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Bigram)) return false;

        Bigram bigram = (Bigram) o;

        if (getFirst() != null ? !getFirst().equals(bigram.getFirst()) : bigram.getFirst() != null) return false;
        if (getSecond() != null ? !getSecond().equals(bigram.getSecond()) : bigram.getSecond() != null) return false;
        return getDecade() != null ? getDecade().equals(bigram.getDecade()) : bigram.getDecade() == null;
    }
}