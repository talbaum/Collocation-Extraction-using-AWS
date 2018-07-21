package mapReduces; //changed

import org.apache.hadoop.mapreduce.Partitioner;
import com.amazonaws.samples.Bigram;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


import java.io.IOException;
import java.util.StringTokenizer;

public class SecondMapReduce {
	//this map reduce is calculating C(w1)
	public static class SecondMapReduceMapper extends Mapper<LongWritable, Text, Bigram, LongWritable> {
		// public SecondMapReduceMapper() {}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer valueIterator = new StringTokenizer(value.toString());
			Text first = new Text(valueIterator.nextToken());
			Text second = new Text(valueIterator.nextToken());
			Text decade = new Text(valueIterator.nextToken());
			Text textNumOccur = new Text(valueIterator.nextToken());
			LongWritable occurrences = new LongWritable(Integer.parseInt(textNumOccur.toString()));
			Bigram oldBigram = new Bigram(first,second,decade);
			Bigram w1Bigram = new Bigram(first,new Text("*"),decade);
			context.write(oldBigram,occurrences); //we write the data from the former map reduce
			context.write(w1Bigram,occurrences);
		}
	}

	public static class SecondMapReducePartitioner extends Partitioner< Bigram, LongWritable > {

		@Override
		public int getPartition(Bigram bigram, LongWritable intWritable, int numReduceTasks) {
			return Integer.parseInt(bigram.getDecade().toString())%numReduceTasks;
			//return Math.abs(bigram.hashCode()) % numPartitions;
		}
	}

	public static class SecondMapReduceReducer extends Reducer<Bigram,LongWritable,Bigram,Text> {
		private long firstWordCounter;
		private Text currentFirstWord; //keep track of the incoming keys

		protected void setup(@SuppressWarnings("rawtypes") Mapper.Context context) throws IOException, InterruptedException {
			firstWordCounter = 0;
			currentFirstWord = new Text("");
		}
		@Override
		public void reduce(Bigram key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
			if(!key.getFirst().equals(currentFirstWord)) {
				currentFirstWord = key.getFirst();
				countValues(values);
			} else {
				if (key.getSecond().toString().equals("*")) {
					countValues(values);
				} else {
					Text Cw1w2 = new Text(values.iterator().next().toString());
					Text Cw1 = new Text(String.valueOf(firstWordCounter));
					context.write(new Bigram(key.getFirst(), key.getSecond(), key.getDecade()), new Text(Cw1w2.toString() + " " + Cw1.toString()));
				}
			}
		}
		private void countValues(Iterable<LongWritable> values) {
			firstWordCounter = 0;
			for (LongWritable value : values) {
				firstWordCounter += value.get();
			}
		}
	}

	public static void main(String[] args) throws Exception, ClassNotFoundException, InterruptedException  {
		Configuration conf = new Configuration();
		Job myJob = new Job(conf, "step2");
		myJob.setJarByClass(SecondMapReduce.class);
		myJob.setMapperClass(SecondMapReduceMapper.class);
		//myJob.setCombinerClass(SecondMapReduceReducer.class);
		myJob.setReducerClass(SecondMapReduceReducer.class);
		myJob.setOutputKeyClass(com.amazonaws.samples.Bigram.class);
		myJob.setOutputValueClass(IntWritable.class);
		//myJob.setOutputFormatClass(TextOutputFormat.class);
		myJob.setMapOutputKeyClass(com.amazonaws.samples.Bigram.class);
		myJob.setMapOutputValueClass(LongWritable.class);
		myJob.setPartitionerClass(SecondMapReducePartitioner.class);
		TextInputFormat.addInputPath(myJob, new Path(args[1]));
		String output=args[2];
		TextOutputFormat.setOutputPath(myJob, new Path(output));
		myJob.waitForCompletion(true);	
	}

}