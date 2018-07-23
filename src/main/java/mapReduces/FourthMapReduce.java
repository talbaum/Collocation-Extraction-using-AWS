package mapReduces;

import org.apache.hadoop.fs.Path;
import com.amazonaws.samples.Bigram;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Partitioner;
import java.util.StringTokenizer;

//calculates N
public class FourthMapReduce {
	//public FourthMapReduce() {}
	public static class FourthMapReduceMapper extends Mapper<LongWritable, Text, Bigram, Text> {
		//public FourthMapReduceMapper() {}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			Text first = new Text(itr.nextToken());
			Text second = new Text(itr.nextToken());
			Text decade = new Text(itr.nextToken());
			Text occurrences = new Text(itr.nextToken());
			Text Cw1 = new Text(itr.nextToken());
			Text Cw2 = new Text(itr.nextToken());
			Text olderData = new Text(occurrences.toString() + " "+Cw1.toString() + " " + Cw2.toString());

			Bigram bigram = new Bigram(first,second,decade);
			Bigram decadeBigram = new Bigram(new Text("*"),new Text("*"), decade);

			context.write(bigram,olderData); //we write the data from the former map reduce
			context.write(decadeBigram,olderData);    
		}
	}

	public static class FourthMapReducePartitioner extends Partitioner< Bigram, Text > {
		@Override
		public int getPartition(Bigram bigram, Text text, int numReduceTasks) {
			return Integer.parseInt(bigram.getDecade().toString())%numReduceTasks;
			//return Math.abs(bigram.hashCode()) % numPartitions;
		}
	}

	public static class FourthMapReduceReducer extends Reducer<Bigram,Text,Bigram,Text> {
		private long N;
		private Text currentDecade; 

		protected void setup(@SuppressWarnings("rawtypes") Mapper.Context context) throws IOException, InterruptedException {
			currentDecade = new Text("");
			N = 0;
		}

		@Override
		public void reduce(Bigram key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
			if(!key.getDecade().equals(currentDecade)) {
				currentDecade = key.getDecade();
				countValues(values);
			} else {
				if (isBothWordStar(key)) {	
					countValues(values);
				} else {
					StringBuffer olderData = new StringBuffer("");
					for (Text value : values) {
						olderData.append(value.toString());
					}                  
					Text likeRatio = calcTheEquation(olderData);
					context.write(new com.amazonaws.samples.Bigram(key.getFirst(),key.getSecond(),key.getDecade()),likeRatio);              
				}
			}
		}

		private void countValues(Iterable<Text> values) {
			N = 0;
			for (Text value : values) {
				StringTokenizer iterator = new StringTokenizer(value.toString());
				N += Long.parseLong(iterator.nextToken());
			}
		}

		private boolean isBothWordStar(Bigram key) {
			return key.getFirst().toString().equals("*") && key.getSecond().toString().equals("*");
		}

		private Text calcTheEquation(StringBuffer data) {
			StringTokenizer itr = new StringTokenizer(data.toString());
			//STEP 44- WITHOUT PRINTS, STEP 4 - WITH PRINTS
			double C12 = Double.parseDouble(itr.nextToken());
			//System.out.print("c12: "+ C12 +" ");
			double C1 = Double.parseDouble(itr.nextToken());
			//System.out.print("c1 is "+ C1+" ");
			double C2 = Double.parseDouble(itr.nextToken());
			//System.out.print("c2 is "+ C2+" ");
			double N1 = Double.parseDouble(String.valueOf(N));
			//System.out.print("N1 is "+ N1+" ");
			double p = C2/N1;
			//System.out.print("p is "+ p+" ");
			double p1 = C12/C1;
			//System.out.print("p1 is "+ p1+" ");
			double p2 = (C2-C12)/(N1-C1);
			//System.out.print("p2 is "+ p2+" ");
			double first_element = log(L(C12,C1,p));
			//System.out.print("first_element is "+ first_element+" ");
			double second_element = log(L((C2-C12),(N1-C1),p));
			//System.out.print("second_element is "+ second_element+" ");
			double third_element = log(L(C12,C1,p1));
			//System.out.print("third_element is "+ third_element+" ");
			double fourth_element = log(L((C2-C12),(N1-C1),p2));
			//System.out.print("fourth_element is "+ fourth_element+" ");
			double likehood = first_element+second_element-third_element-fourth_element;		
			double minus2loglikehood= -2* likehood;
			//System.out.println("minus2loglikehood is "+ minus2loglikehood);
			return new Text(String.valueOf(minus2loglikehood));
		}

		private double L(double k,double n,double x) {
			return Math.pow(x, k)*Math.pow((1-x), (n-k)); 
		}

		private static double log( double a ){
			return Math.log(a) / Math.log(2);
		}
	}

	public static void main(String[] args) throws Exception, ClassNotFoundException, InterruptedException  {
		Configuration conf = new Configuration();
		Job myJob = new Job(conf, "step4");
		myJob.setJarByClass(FourthMapReduce.class);
		myJob.setMapperClass(FourthMapReduceMapper.class);
		//myJob.setCombinerClass(FourthMapReduceReducer.class);
		myJob.setReducerClass(FourthMapReduceReducer.class);
		myJob.setOutputKeyClass(com.amazonaws.samples.Bigram.class);
		myJob.setOutputValueClass(Text.class);
		//myJob.setOutputFormatClass(TextOutputFormat.class);
		myJob.setMapOutputKeyClass(com.amazonaws.samples.Bigram.class);
		myJob.setMapOutputValueClass(Text.class);
		myJob.setPartitionerClass(FourthMapReducePartitioner.class);

		TextInputFormat.addInputPath(myJob, new Path(args[1]));
		String output=args[2];
		TextOutputFormat.setOutputPath(myJob, new Path(output));
		myJob.waitForCompletion(true);	
	}

}