package mapReduces;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import java.io.IOException;
import java.util.StringTokenizer;
import com.amazonaws.samples.BigramFinal;

import mapReduces.FourthMapReduce.FourthMapReduceMapper;
import mapReduces.FourthMapReduce.FourthMapReducePartitioner;
import mapReduces.FourthMapReduce.FourthMapReduceReducer;

public class FifthMapReduce {


//this map reduce calculates N

    public FifthMapReduce() {}
  //TODO: all of this class
    public static class FifthMapReduceMapper extends Mapper<LongWritable, Text, BigramFinal, Text> {
        public FifthMapReduceMapper() {}

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	  StringTokenizer itr = new StringTokenizer(value.toString());
              Text first = new Text(itr.nextToken());
              Text second = new Text(itr.nextToken());
              Text decade = new Text(itr.nextToken());
              Text likehoodText = new Text(itr.nextToken());

              BigramFinal bigram = new BigramFinal(first,second,decade,likehoodText); //likehood has to be here so it will be sorted
              BigramFinal bigramByDecade = new BigramFinal(new Text("*"),new Text("*"),decade,new Text("~"));

              context.write(bigram,likehoodText); //we write the data from the former map reduce
              context.write(bigramByDecade,likehoodText);
        }
    }

    public static class FifthMapReducePartitioner extends Partitioner< BigramFinal, Text > {

        @Override
        public int getPartition(BigramFinal bigram, Text text, int numReduceTasks) {
                       return Integer.parseInt(bigram.getDecade().toString())%numReduceTasks;
                       //return Math.abs(bigram.hashCode()) % numPartitions;
                    }
         }


    public static class FifthMapReduceReducer extends Reducer<BigramFinal,Text,BigramFinal,Text> {
        private double likehoodCounter;

        //keep track of the incoming keys
        private Text currentDecade;

        protected void setup(@SuppressWarnings("rawtypes") Mapper.Context context) throws IOException, InterruptedException {
        	likehoodCounter = 0;
            currentDecade = new Text("");
        }

        @Override
        public void reduce(BigramFinal key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            if(!key.getDecade().equals(currentDecade)) {
                currentDecade = key.getDecade();
                likehoodCounter = 0;
               
            } else {
            	likehoodCounter++;
                if (key.getFirst().toString().equals("*") && key.getSecond().toString().equals("*")) {
                    
                    double sum = 0;
                    for (Text value : values) {
                        StringTokenizer itr = new StringTokenizer(value.toString());
                        sum += (double) Double.parseDouble(itr.nextToken());
                    }
                    
                } else {
                    StringBuffer dataToTransfer = new StringBuffer("");
                    for (Text value : values) {
                        dataToTransfer.append(value.toString());
                    }

                    StringTokenizer itr = new StringTokenizer(dataToTransfer.toString());

                    //now we have  sumOfAllNormalizedPMI!
                    String i = itr.nextToken();
                    double likehood = Double.parseDouble(i);
                    Text likehoodTxt = new Text(String.valueOf(likehood));
                    

                    if(likehoodCounter<=10)	
                    	context.write(new BigramFinal(key.getFirst(),key.getSecond(),key.getDecade()), likehoodTxt);                    
                }
            }
        }
    }
    
    public static void main(String[] args) throws Exception, ClassNotFoundException, InterruptedException  {
    	Configuration conf = new Configuration();
    	Job myJob = new Job(conf, "step5");
    	myJob.setJarByClass(FifthMapReduce.class);
    	myJob.setMapperClass(FifthMapReduceMapper.class);
    	//myJob.setCombinerClass(FifthMapReduceReducer.class);
    	myJob.setReducerClass(FifthMapReduceReducer.class);
    	myJob.setOutputKeyClass(com.amazonaws.samples.BigramFinal.class);
    	myJob.setOutputValueClass(Text.class);
    	//myJob.setOutputFormatClass(TextOutputFormat.class);
    	myJob.setMapOutputKeyClass(com.amazonaws.samples.BigramFinal.class);
    	myJob.setMapOutputValueClass(Text.class);
    	myJob.setPartitionerClass(FifthMapReducePartitioner.class);

    	TextInputFormat.addInputPath(myJob, new Path(args[1]));
    	String output=args[2];
    	TextOutputFormat.setOutputPath(myJob, new Path(output));
    	myJob.waitForCompletion(true);	
    }
    
    
}