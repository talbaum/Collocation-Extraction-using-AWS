package com.amazonaws.samples;
import java.io.IOException;
import java.util.StringTokenizer;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import com.hadoop.mapreduce.LzoTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
 
public class NGramsReading { 
 
public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
       String[] splitted = value.toString().split("\t");
       if (splitted.length < 4) { return; } /* malformed line, skip it. */
       String ngram = splitted[0];
       String count = splitted[1];
       String pages = splitted[2];
       String books = splitted[3];
       String[] ngram_words = ngram.split(" ");
       if (ngram_words.length != 5) { return; }

       context.write(new Text(ngram_words[2]), new IntWritable(Integer.valueOf(count)));
    }
  }
 
  public static class ReduceClass extends Reducer<Text,IntWritable,Text,IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      if (sum > 10000) 
         context.write(key, new IntWritable(sum)); 
    }
  }
 
public static class PartitionerClass extends Partitioner<Text, IntWritable> {
  // Partitioner implementation: divide the keys according to their language: Hebrew - 1, Other - 0
  public int getPartition(Text key, IntWritable value, int numPartitions) {
    return getLanguage(key) % numPartitions;
  }
 
  private int getLanguage(Text key) {
     if (key.getLength() > 0) {
        int c = key.charAt(0);
        if (c >= Long.decode("0x05D0").longValue() && c <= Long.decode("0x05EA").longValue()) // Hebrew character
           return 1;
     }
     return 0;
  }
}
 
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    //conf.set("mapred.map.tasks","10");
    //conf.set("mapred.reduce.tasks","2");
    Job job = new Job(conf, "word count");
    job.setJarByClass(NGramsReading.class);
    job.setMapperClass(MapClass.class);
    //job.setPartitionerClass(PartitionerClass.class);
    job.setCombinerClass(ReduceClass.class);
    job.setReducerClass(ReduceClass.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
 
}
