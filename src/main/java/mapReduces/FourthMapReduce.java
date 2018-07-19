package mapReduces;

import com.amazonaws.samples.Bigram;

import mapReduces.ThirdMapReduce.ThirdMapReduceMapper;
import mapReduces.ThirdMapReduce.ThirdMapReducePartitioner;
import mapReduces.ThirdMapReduce.ThirdMapReduceReducer;

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
import org.apache.hadoop.mapreduce.Partitioner;
import java.util.StringTokenizer;

//this map reduce calculates N
public class FourthMapReduce {
	//TODO: all of this class
    public FourthMapReduce() {}

    public static class FourthMapReduceMapper extends Mapper<LongWritable, Text, Bigram, Text> {

        public FourthMapReduceMapper() {}

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	StringTokenizer itr = new StringTokenizer(value.toString());
            Text first = new Text(itr.nextToken());
            Text second = new Text(itr.nextToken());
            Text decade = new Text(itr.nextToken());
            Text numberOfOccurrences = new Text(itr.nextToken());
            Text Cw1 = new Text(itr.nextToken());
            Text Cw2 = new Text(itr.nextToken());
            Text dataToTransfer = new Text(numberOfOccurrences.toString() + " "+Cw1.toString() + " " + Cw2.toString());

            Bigram bigram = new Bigram(first,second,decade);
            Bigram bigramByDecade = new Bigram(new Text("*"),new Text("*"), decade);

            context.write(bigram,dataToTransfer); //we write the data from the former map reduce
            context.write(bigramByDecade,dataToTransfer);
            
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
        private Text currentDecade;  //keep track of the incoming keys

        protected void setup(@SuppressWarnings("rawtypes") Mapper.Context context) throws IOException, InterruptedException {
            N = 0;
            currentDecade = new Text("");
        }
        private double L(double k,double n,double x) {//Maybe change too float because too large.     change to float?
        	return Math.pow(x, k)*Math.pow((1-x), (n-k)); 
        }
        private static double log( double a )//change to float?
        {
        	return Math.log(a) / Math.log(2);
        	}
        @Override
        public void reduce(Bigram key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
        	if(!key.getDecade().equals(currentDecade)) {
                currentDecade = key.getDecade();
                N = 0;
                long sum = 0;
                for (Text value : values) {
                    StringTokenizer itr = new StringTokenizer(value.toString());
                    sum += Long.parseLong(itr.nextToken());
                }
                N += sum;
            } else {
                if (key.getFirst().toString().equals("*") && key.getSecond().toString().equals("*")) {
                    N = 0;
                    long sum = 0;
                    for (Text value : values) {
                        StringTokenizer itr = new StringTokenizer(value.toString());
                        sum += Long.parseLong(itr.nextToken());
                    }
                    N += sum;
                } else {
                    StringBuffer dataToTransfer = new StringBuffer("");
                    for (Text value : values) {
                        dataToTransfer.append(value.toString());
                    }

                    StringTokenizer itr = new StringTokenizer(dataToTransfer.toString());

                    //now we have N, we can calculate the likehood!

                    double C12 = Double.parseDouble(itr.nextToken());
                    double C1 = Double.parseDouble(itr.nextToken());
                    double C2 = Double.parseDouble(itr.nextToken());
                    double N1 = Double.parseDouble(String.valueOf(N));
                    double p = C2/N1;
                    double p1 = C12/C1;
                    double p2 = (C2-C12)/(N1-C1);
                    double first_element = log(L(C12,C1,p));
                    double second_element = log(L((C2-C12),(N1-C1),p));
                    double third_element = log(L(C12,C1,p1));
                    double fourth_element = log(L((C2-C12),(N1-C1),p2));
                    double likehood = first_element+second_element-third_element-fourth_element;
                    Text like = new Text(String.valueOf(likehood));
                    context.write(new com.amazonaws.samples.Bigram(key.getFirst(),key.getSecond(),key.getDecade()),like);
                    
                }
                
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
}