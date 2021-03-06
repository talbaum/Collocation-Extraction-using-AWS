package com.amazonaws.samples;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;


public class Main {
	  private static final String FIRST_OUTPUT = "s3n://ass2talstas//NoCombine//first_output";
	    private static final String SECOND_OUTPUT = "s3n://ass2talstas//NoCombine//second_output";
	    private static final String THIRD_OUTPUT = "s3n://ass2talstas//NoCombine//third_output2";
	    private static final String FOURTH_OUTPUT = "s3n://ass2talstas//NoCombine//fourth_output";
	    public static final String FINAL_OUTPUT = "s3n://ass2talstas//NoCombine//final_output";
	    
	public static void main(String[]args){
		
		  AWSCredentialsProvider credentials = new AWSStaticCredentialsProvider(
				new EnvironmentVariableCredentialsProvider().getCredentials());	
	
		System.out.println("Create the EMR...");
		AmazonElasticMapReduce emr= AmazonElasticMapReduceClientBuilder.standard()
				.withCredentials(credentials)
				.withRegion("us-east-1")
				.build();
		 
		System.out.println("Clusters detail: " + emr.listClusters());
		String lang=args[0];
		String ngramLink;
		if(lang.equals("eng"))
			ngramLink="s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data";
		else
			ngramLink= "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";

		HadoopJarStepConfig step1 = new HadoopJarStepConfig()
				.withJar("s3://ass2talstas/step1NoCombine.jar")
				.withArgs("FirstMapReduce",ngramLink,lang,FIRST_OUTPUT);

		StepConfig stepOne = new StepConfig()
				.withName("FirstMapReduce")
				.withHadoopJarStep(step1)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		HadoopJarStepConfig step2 = new HadoopJarStepConfig()
				.withJar("s3://ass2talstas/step2.jar")
				.withArgs("SecondMapReduce",FIRST_OUTPUT,SECOND_OUTPUT);

		StepConfig stepTwo = new StepConfig()
				.withName("SecondMapReduce")
				.withHadoopJarStep(step2)
				.withActionOnFailure("TERMINATE_JOB_FLOW");
	
		HadoopJarStepConfig step3 = new HadoopJarStepConfig()
				.withJar("s3://ass2talstas/step3.jar")
				.withArgs("ThirdMapReduce",SECOND_OUTPUT,THIRD_OUTPUT);

		StepConfig stepThree = new StepConfig()
				.withName("ThirdMapReduce")
				.withHadoopJarStep(step3)
				.withActionOnFailure("TERMINATE_JOB_FLOW");
	
		HadoopJarStepConfig step4 = new HadoopJarStepConfig()
				.withJar("s3://ass2talstas/step4.jar")
				.withArgs("FourthMapReduce",THIRD_OUTPUT,FOURTH_OUTPUT);

		StepConfig stepFour = new StepConfig()
				.withName("FourthMapReduce")
				.withHadoopJarStep(step4)
				.withActionOnFailure("TERMINATE_JOB_FLOW");
	
		HadoopJarStepConfig step5 = new HadoopJarStepConfig()
				.withJar("s3://ass2talstas/step5.jar")
				.withArgs("FifthMapReduce",FOURTH_OUTPUT,FINAL_OUTPUT);

		StepConfig stepFive = new StepConfig()
				.withName("FifthMapReduce")
				.withHadoopJarStep(step5)
				.withActionOnFailure("TERMINATE_JOB_FLOW");
	
		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
			    .withInstanceCount(8)
			    .withMasterInstanceType(InstanceType.M3Xlarge.toString())
				.withSlaveInstanceType(InstanceType.M3Xlarge.toString())
			    .withHadoopVersion("2.6.0").withEc2KeyName("Talbaum1")
			    .withKeepJobFlowAliveWhenNoSteps(false)
				.withPlacement(new PlacementType("us-east-1a"));

		RunJobFlowRequest request = new RunJobFlowRequest()
				.withName("ass2")                                   
				.withInstances(instances)
				.withSteps(stepOne,stepTwo,stepThree,stepFour,stepFive)
				.withLogUri("s3n://ass2talstas/logs/")
				.withServiceRole("EMR_DefaultRole")
				.withJobFlowRole("EMR_EC2_DefaultRole")
				.withReleaseLabel("emr-5.11.0");
				 
		RunJobFlowResult result = emr.runJobFlow(request);
		String id=result.getJobFlowId();
		System.out.println("The cluster id is: "+id);

	}
}