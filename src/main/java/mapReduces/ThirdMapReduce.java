package mapReduces;

import org.apache.hadoop.mapreduce.Partitioner;
import com.amazonaws.samples.Bigram;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.StringBuffer;
//this map reduce is calculating c(W2)

public class ThirdMapReduce {
	//TODO: all of this class
    //public ThirdMapReduce() {}

    public static class ThirdMapReduceMapper extends Mapper<LongWritable, Text, Bigram, Text> {

        //public ThirdMapReduceMapper() {}

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	  StringTokenizer itr = new StringTokenizer(value.toString());
              Text first = new Text(itr.nextToken());
              Text second = new Text(itr.nextToken());
              Text decade = new Text(itr.nextToken());
              Text occurrences = new Text(itr.nextToken());
              Text Cw1 = new Text(itr.nextToken());
              Text olderData = new Text(occurrences.toString() + " "+Cw1.toString());

              //we reverse it! (For the sorting)
              Bigram bigram = new Bigram(second,first,decade);
              Bigram bigramStar = new Bigram(second,new Text("*"),decade);

              //we also pass the data from the former map reduce
              context.write(bigram,olderData);
              context.write(bigramStar,olderData); 
        }
    }

    public static class ThirdMapReducePartitioner extends Partitioner< Bigram, Text > {

    	@Override
        public int getPartition(Bigram bigram, Text text, int numReduceTasks) {
                        return Integer.parseInt(bigram.getDecade().toString())%numReduceTasks;
                    	//return Math.abs(bigram.hashCode()) % numPartitions;
                   }
        }

    public static class ThirdMapReduceReducer extends Reducer<Bigram,Text,Bigram,Text> {
    	private long secondWordCounter;
        private Text currentSecondWord;  //keep track of the incoming keys

        protected void setup(@SuppressWarnings("rawtypes") Mapper.Context context) throws IOException, InterruptedException {
            secondWordCounter = 0;
            currentSecondWord = new Text("");
        }

        @Override
        public void reduce(Bigram key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            if(!key.getFirst().equals(currentSecondWord)) {
                currentSecondWord = key.getFirst();
                //secondWordCounter = 0;
               // long sum = 0;
                countValues(values);
                //secondWordCounter += sum;
            } else {
                if (key.getSecond().toString().equals("*")) {
                    //secondWordCounter = 0;
                    //long sum = 0;
                    countValues(values);
                    //secondWordCounter += sum;
                } else {
                    StringBuffer olderData = new StringBuffer("");
                    for (Text value : values) {
                    	olderData.append(value.toString());
                    }
                    Text Cw2 = new Text(String.valueOf(secondWordCounter));
                    context.write(new Bigram(key.getSecond(), key.getFirst(), key.getDecade()), new Text(olderData.toString() + " " + Cw2.toString()));
                }
            }
    }

		private void countValues(Iterable<Text> values) {
			 secondWordCounter = 0;
		    for (Text value : values) {
                StringTokenizer iterator = new StringTokenizer(value.toString());
                secondWordCounter += Long.parseLong(iterator.nextToken());
            }
			
		}
}
    
    public static void main(String[] args) throws Exception, ClassNotFoundException, InterruptedException  {
    	Configuration conf = new Configuration();
    	Job myJob = new Job(conf, "step3");
    	myJob.setJarByClass(ThirdMapReduce.class);
    	myJob.setMapperClass(ThirdMapReduceMapper.class);
    	myJob.setCombinerClass(ThirdMapReduceReducer.class);
    	myJob.setReducerClass(ThirdMapReduceReducer.class);
    	myJob.setOutputKeyClass(com.amazonaws.samples.Bigram.class);
    	myJob.setOutputValueClass(Text.class);
    	//myJob.setOutputFormatClass(TextOutputFormat.class);
    	myJob.setMapOutputKeyClass(com.amazonaws.samples.Bigram.class);
    	myJob.setMapOutputValueClass(Text.class);
    	myJob.setPartitionerClass(ThirdMapReducePartitioner.class);

    	TextInputFormat.addInputPath(myJob, new Path(args[1]));
    	String output=args[2];
    	TextOutputFormat.setOutputPath(myJob, new Path(output));
    	myJob.waitForCompletion(true);	
    }
}

