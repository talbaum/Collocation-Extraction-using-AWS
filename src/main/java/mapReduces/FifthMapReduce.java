package mapReduces;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import java.io.IOException;
import java.util.StringTokenizer;
import com.amazonaws.samples.BigramFinal;

public class FifthMapReduce {


//this map reduce calculates N

    public FifthMapReduce() {}
  //TODO: all of this class
    public static class FifthMapReduceMapper extends Mapper<LongWritable, Text, BigramFinal, Text> {
        public FifthMapReduceMapper() {}

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
        }
    }

    public static class FifthMapReducePartitioner extends Partitioner< BigramFinal, Text > {

        @Override
        public int getPartition(BigramFinal bigram, Text text, int numReduceTasks) {
                //TODO       
        		return 1;
                    }
         }

    public static class FifthMapReduceReducer extends Reducer<BigramFinal,Text,BigramFinal,Text> {
        
        @Override
        public void reduce(BigramFinal key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            
            }
        }
    }
