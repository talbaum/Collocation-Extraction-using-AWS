package mapReduces;

import org.apache.hadoop.mapreduce.Partitioner;
import com.amazonaws.samples.Bigram;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


public class SecondMapReduce {
	//TODO: all of this class
//this map reduce is calculating C(w1)

    public SecondMapReduce() {}

    public static class SecondMapReduceMapper extends Mapper<LongWritable, Text, Bigram, LongWritable> {
        public SecondMapReduceMapper() {}

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
 
        }
    }

    public static class SecondMapReducePartitioner extends Partitioner< Bigram, LongWritable > {

        @Override
        public int getPartition(Bigram bigram, LongWritable intWritable, int numReduceTasks) {
                   //TODO   
        			return 0;
                    }
         }

    public static class SecondMapReduceReducer extends Reducer<Bigram,LongWritable,Bigram,Text> {

            }
        
}