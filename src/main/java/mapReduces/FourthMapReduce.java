package mapReduces;

import com.amazonaws.samples.Bigram;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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
            
        }
    }

    public static class FourthMapReducePartitioner extends Partitioner< Bigram, Text > {

        @Override
        public int getPartition(Bigram bigram, Text text, int numReduceTasks) {
                //TODO	        
        		return 1;
                    }
        }

    public static class FourthMapReduceReducer extends Reducer<Bigram,Text,Bigram,Text> {

       

        @Override
        public void reduce(Bigram key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            
            
        }
    }
}