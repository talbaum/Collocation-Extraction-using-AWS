package com.amazonaws.samples;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

import java.util.List;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
//import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
public class main {

	public static void main(String[] args) {
		
		for(String arg :args)
			System.out.println(arg);
		
		String[] stopWords=getStopWords(args[0]);
		for( String w : stopWords)
			System.out.println(w);
	}
		/*
		AWSCredentialsProvider credentials = new AWSStaticCredentialsProvider(
				new EnvironmentVariableCredentialsProvider().getCredentials());
		AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
		 
		HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
		    .withJar("s3n://yourbucket/yourfile.jar") // This should be a full map reduce application.
		    .withMainClass("some.pack.MainClass")
		    .withArgs("s3n://yourbucket/input/", "s3n://yourbucket/output/");
		 
		StepConfig stepConfig = new StepConfig()
		    .withName("stepname")
		    .withHadoopJarStep(hadoopJarStep)
		    .withActionOnFailure("TERMINATE_JOB_FLOW");
		 
		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
		    .withInstanceCount(2)
		    .withMasterInstanceType(InstanceType.M1Small.toString())
		    .withSlaveInstanceType(InstanceType.M1Small.toString())
		    .withHadoopVersion("2.6.0").withEc2KeyName("yourkey")
		    .withKeepJobFlowAliveWhenNoSteps(false)
		    .withPlacement(new PlacementType("us-east-1a"));
		 
		RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
		    .withName("jobname")
		    .withInstances(instances)
		    .withSteps(stepConfig)
		    .withLogUri("s3n://yourbucket/logs/");
		 
		RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
		String jobFlowId = runJobFlowResult.getJobFlowId();
		System.out.println("Ran job flow with id: " + jobFlowId);

	}

	*/

		private static String[] getStopWords(String lang) {
			if(lang.equals("eng")) {
				return readWordFromFile("EnglishStopWords.txt");
			}
			else if(lang.equals("heb")) {
				return readWordFromFile("HebrewStopWords.txt");
			}
			else
				return null;	
		}

		private static String[] readWordFromFile(String file) {
			Path filePath = new File(file).toPath();
			Charset charset = Charset.forName("utf-8");        
			List<String> stringList;
			try {
				stringList = Files.readAllLines(filePath, charset);
				String[] stopWords = stringList.toArray(new String[]{});
				return stopWords;
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
	}
}
