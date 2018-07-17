package com.amazonaws.samples;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;


public class mainE {
	public static void main(String[]args){
		AWSCredentialsProvider credentials = new AWSStaticCredentialsProvider(
				new EnvironmentVariableCredentialsProvider().getCredentials());		

		System.out.println("creating a emr");
		AmazonElasticMapReduce emr= AmazonElasticMapReduceClientBuilder.standard()
				.withCredentials(credentials)
				.withRegion("us-east-1")
				.build();
		 
		System.out.println(emr.listClusters());
		String lang=args[0];
		String ngramLink;
		if(lang.equals("eng"))
			ngramLink="s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data";
		else
			ngramLink= "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";
			 			
		/*
        step1
		 */
		HadoopJarStepConfig step1 = new HadoopJarStepConfig()
				.withJar("s3://ass2talstas/step1.jar")
				.withArgs("FirstMapReduce",lang,ngramLink);

		StepConfig stepOne = new StepConfig()
				.withName("FirstMapReduce")
				.withHadoopJarStep(step1)
				.withActionOnFailure("TERMINATE_JOB_FLOW");
		/*
        step2
		 */
		HadoopJarStepConfig step2 = new HadoopJarStepConfig()
				.withJar("s3://ass2talstas/step2.jar")
				.withArgs("SecondMapReduce",lang,ngramLink);

		StepConfig stepTwo = new StepConfig()
				.withName("SecondMapReduce")
				.withHadoopJarStep(step2)
				.withActionOnFailure("TERMINATE_JOB_FLOW");
		/*
        step3
		 */
		HadoopJarStepConfig step3 = new HadoopJarStepConfig()
				.withJar("s3://ass2talstas/step3.jar")
				.withArgs("ThirdMapReduce",lang,ngramLink);

		StepConfig stepThree = new StepConfig()
				.withName("ThirdMapReduce")
				.withHadoopJarStep(step3)
				.withActionOnFailure("TERMINATE_JOB_FLOW");
		/*
        step4
		 */
		HadoopJarStepConfig step4 = new HadoopJarStepConfig()
				.withJar("s3://ass2talstas/step4.jar")
				.withArgs("FourthMapReduce");

		StepConfig stepFour = new StepConfig()
				.withName("FourthMapReduce")
				.withHadoopJarStep(step4)
				.withActionOnFailure("TERMINATE_JOB_FLOW");
		/*
        step5
		 */
		HadoopJarStepConfig step5 = new HadoopJarStepConfig()
				.withJar("s3://ass2talstas/step5.jar")
				.withArgs("FifthMapReduce");

		StepConfig stepFive = new StepConfig()
				.withName("FifthMapReduce")
				.withHadoopJarStep(step5)
				.withActionOnFailure("TERMINATE_JOB_FLOW");
		/*
        step6
		
		HadoopJarStepConfig step6 = new HadoopJarStepConfig()
				.withJar("s3://ass2talstas/step6.jar")
				.withArgs("step6","null","s3n://assignment2dspmor//outputAssignment2");

		StepConfig stepSix = new StepConfig()
				.withName("step6")
				.withHadoopJarStep(step6)
				.withActionOnFailure("TERMINATE_JOB_FLOW");
		 */
		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
			    .withInstanceCount(2)
			    .withMasterInstanceType(InstanceType.M1Small.toString())
			    .withSlaveInstanceType(InstanceType.M1Small.toString())
			    .withHadoopVersion("2.6.0").withEc2KeyName("Talbaum1")
			    .withKeepJobFlowAliveWhenNoSteps(false)
				.withPlacement(new PlacementType("us-east-1a"));

		System.out.println("give the cluster all our steps");
		RunJobFlowRequest request = new RunJobFlowRequest()
				.withName("ass2")                                   
				.withInstances(instances)
				.withSteps(stepOne)
				.withLogUri("s3n://ass2talstas/logs/")
				.withServiceRole("EMR_DefaultRole")
				.withJobFlowRole("EMR_EC2_DefaultRole")
				.withReleaseLabel("emr-5.11.0");
				 
		RunJobFlowResult result = emr.runJobFlow(request);
		String id=result.getJobFlowId();
		System.out.println("our cluster id: "+id);

	}
}