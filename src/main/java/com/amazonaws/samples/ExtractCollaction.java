package com.amazonaws.samples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import mapReduces.FourthMapReduce;
import mapReduces.ThirdMapReduce;

//TODO: change this class 
public class ExtractCollaction {

    private static final String FIRST_OUTPUT = "s3n://ass2talstas/first_output";
    private static final String SECOND_OUTPUT = "s3n://ass2talstas/second_output";
    private static final String THIRD_OUTPUT = "s3n://ass2talstas/third_output";
    private static final String FOURTH_OUTPUT = "s3n://ass2talstas/fourth_output";
    public static final String FINAL_OUTPUT = "s3n://ass2talstas/final_output";


    @SuppressWarnings("unchecked")
	public static boolean setAndRunMapReduceJob (String jobName,Configuration conf, Class<ExtractCollaction> MapReduceClass,Class Mapper, Class Reducer,
                                          Class MapOutputKey,Class MapOutputValue,Class ReduceOutputKey, Class ReduceOutputValue,
                                          String Input, String Output, boolean isLZO, Class partitionerClass) throws Exception{
        Job myJob = new Job(conf, jobName);
        myJob.setJarByClass(MapReduceClass);
        if(Mapper != null) myJob.setMapperClass(Mapper);
        if(Reducer != null) myJob.setReducerClass(Reducer);

        //Mapper`s output
        myJob.setMapOutputKeyClass(MapOutputKey);
        myJob.setMapOutputValueClass(MapOutputValue);

        if(isLZO) myJob.setInputFormatClass(SequenceFileInputFormat.class);

        //Reducer`s output
        if(ReduceOutputKey != null) myJob.setOutputKeyClass(ReduceOutputKey);
        if(ReduceOutputValue != null) myJob.setOutputValueClass(ReduceOutputValue);
        if(partitionerClass != null) myJob.setPartitionerClass(partitionerClass);

        TextInputFormat.addInputPath(myJob, new Path(Input));
        TextOutputFormat.setOutputPath(myJob, new Path(Output));

        return myJob.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        if (args.length>2 || args.length<1) {
            System.out.println("The number of arguments is suppose to be 2 at most");
            return;
        }

        String language = args[0];
        String INPUT = args[1];
        System.out.println("Language received = " + language);
        System.out.println("Input received = " + INPUT);
        Configuration conf = new Configuration();
        conf.set("language",language);
        /*
        conf.set("mapreduce.map.java.opts","-Xmx512m");
        conf.set("mapreduce.reduce.java.opts","-Xmx1536m");
        conf.set("mapreduce.map.memory.mb","768");
        conf.set("mapreduce.reduce.memory.mb","2048");
        conf.set("yarn.app.mapreduce.am.resource.mb","2048");
        conf.set("yarn.scheduler.minimum-allocation-mb","256");
        conf.set("yarn.scheduler.maximum-allocation-mb","12288");
        conf.set("yarn.nodemanager.resource.memory-mb","12288");
        conf.set("mapreduce.reduce.shuffle.memory.limit.percent","0.5");
*/
        boolean waitForJobComletion = setAndRunMapReduceJob("FirstMapReduce",conf, com.amazonaws.samples.ExtractCollaction.class,
                mapReduces.FirstMapReduce.FirstMapReduceMapper.class, mapReduces.FirstMapReduce.FirstMapReduceReducer.class,
                com.amazonaws.samples.Bigram.class,LongWritable.class,
                com.amazonaws.samples.Bigram.class,LongWritable.class,
                INPUT,FIRST_OUTPUT,true,null);

        if (waitForJobComletion) {
            waitForJobComletion = setAndRunMapReduceJob("SecondMapReduce", conf, com.amazonaws.samples.ExtractCollaction.class,
                    mapReduces.SecondMapReduce.SecondMapReduceMapper.class, mapReduces.SecondMapReduce.SecondMapReduceReducer.class,
                    com.amazonaws.samples.Bigram.class, LongWritable.class,
                    com.amazonaws.samples.Bigram.class, IntWritable.class,
                    FIRST_OUTPUT, SECOND_OUTPUT,false,mapReduces.SecondMapReduce.SecondMapReducePartitioner.class);
            if (waitForJobComletion) {
                waitForJobComletion = setAndRunMapReduceJob("ThirdMapReduce", conf, com.amazonaws.samples.ExtractCollaction.class,
                        ThirdMapReduce.ThirdMapReduceMapper.class, ThirdMapReduce.ThirdMapReduceReducer.class,
                        com.amazonaws.samples.Bigram.class, Text.class,
                        com.amazonaws.samples.Bigram.class, Text.class,
                        SECOND_OUTPUT, THIRD_OUTPUT,false,mapReduces.ThirdMapReduce.ThirdMapReducePartitioner.class);

                if (waitForJobComletion) {
                    waitForJobComletion = setAndRunMapReduceJob("FourthMapReduce", conf, com.amazonaws.samples.ExtractCollaction.class,
                            FourthMapReduce.FourthMapReduceMapper.class, FourthMapReduce.FourthMapReduceReducer.class,
                            com.amazonaws.samples.Bigram.class, Text.class,
                            com.amazonaws.samples.Bigram.class, Text.class,
                            THIRD_OUTPUT, FOURTH_OUTPUT,false,mapReduces.FourthMapReduce.FourthMapReducePartitioner.class);
                    if (waitForJobComletion) {
                        waitForJobComletion = setAndRunMapReduceJob("FifthMapReduce", conf, com.amazonaws.samples.ExtractCollaction.class,
                                mapReduces.FifthMapReduce.FifthMapReduceMapper.class, mapReduces.FifthMapReduce.FifthMapReduceReducer.class,
                                com.amazonaws.samples.BigramFinal.class, Text.class,
                                com.amazonaws.samples.BigramFinal.class, Text.class,
                                FOURTH_OUTPUT, FINAL_OUTPUT,false,mapReduces.FifthMapReduce.FifthMapReducePartitioner.class);
                        if (waitForJobComletion) {
                            System.out.println("ExtractCollaction :: Done running all map reduces successfully!");
                            return;
                        }
                    }
                }
            }
        }
        System.out.println("ExtractCollaction :: an error has occurred during one of the jobs.");
        return;
    }
}



