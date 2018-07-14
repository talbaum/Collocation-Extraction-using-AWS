package mapReduces;

import org.apache.hadoop.mapreduce.Partitioner;
import com.amazonaws.samples.Bigram;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.StringBuffer;
//this map reduce is calculating c(W2)

public class ThirdMapReduce {
	//TODO: all of this class
    public ThirdMapReduce() {}

    public static class ThirdMapReduceMapper extends Mapper<LongWritable, Text, Bigram, Text> {

        public ThirdMapReduceMapper() {}

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
        }
    }

    public static class ThirdMapReducePartitioner extends Partitioner< Bigram, Text > {

        @Override
        public int getPartition(Bigram bigram, Text text, int numReduceTasks) {
                       //TODO
        				return 1;
                   }
        }

    public static class ThirdMapReduceReducer extends Reducer<Bigram,Text,Bigram,Text> {

  

        @Override
        public void reduce(Bigram key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
           
        }
    }
}

