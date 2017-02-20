
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.ClusterState;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.ListClustersRequest;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;


public class DSPS_WORD_RELATED {
	
	public static void main(String args[])throws Exception { 

		AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("./credentials", "default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (/users/studs/bsc/2015/davidzag/.aws/credentials), and is in valid format.",
                    e);
        }
		
		AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
		
//                                       StepOne                                                              //		
/*************************************************************************************************************/  
		HadoopJarStepConfig hadoopJarStepFirst = new HadoopJarStepConfig()
			.withJar("s3n://deddy-dan-dsps162/Project2/Source/StepOne.jar") // This should be a full map reduce application.
			.withMainClass("Step.One.StepOne")
			.withArgs("s3n://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/5gram/data", "s3n://deddy-dan-dsps162/Project2/Outputs/Out1");
		
		StepConfig stepConfigFirst = new StepConfig()
		    .withName("PairingStep")
			.withHadoopJarStep(hadoopJarStepFirst)
			.withActionOnFailure("TERMINATE_JOB_FLOW");

//    									  StepTwo                                                             //	
/*************************************************************************************************************/  

		HadoopJarStepConfig hadoopJarStepSec = new HadoopJarStepConfig()
			.withJar("s3n://deddy-dan-dsps162/Project2/Source/StepTwo.jar") // This should be a full map reduce application.
			.withMainClass("Step.Two.StepTwo")
			.withArgs("s3n://deddy-dan-dsps162/Project2/Outputs/Out1", "s3n://deddy-dan-dsps162/Project2/Outputs/Out2");

		StepConfig stepConfigSec = new StepConfig()
		    .withName("Word1Count")
			.withHadoopJarStep(hadoopJarStepSec)
			.withActionOnFailure("TERMINATE_JOB_FLOW");
		
//										  StepThree                                                           //
/*************************************************************************************************************/
		
		HadoopJarStepConfig hadoopJarStepThird = new HadoopJarStepConfig()
			.withJar("s3n://deddy-dan-dsps162/Project2/Source/StepThree.jar") // This should be a full map reduce application.
			.withMainClass("Step.Three.StepThree")
			.withArgs("s3n://deddy-dan-dsps162/Project2/Outputs/Out2", "s3n://deddy-dan-dsps162/Project2/Outputs/Out3");
	
		StepConfig stepConfigThird = new StepConfig()
		    .withName("Word2Count")
			.withHadoopJarStep(hadoopJarStepThird)
			.withActionOnFailure("TERMINATE_JOB_FLOW");

//										  StepFour                                                           //
/*************************************************************************************************************/
			
		HadoopJarStepConfig hadoopJarStepFourth = new HadoopJarStepConfig()
		.withJar("s3n://deddy-dan-dsps162/Project2/Source/StepFour.jar") 	// This should be a full map reduce application.
		.withMainClass("Step.Four.StepFour")
		.withArgs("s3n://deddy-dan-dsps162/Project2/Outputs/Out3", "s3n://deddy-dan-dsps162/Project2/Outputs/Out4", args[0]);

		StepConfig stepConfigFourth = new StepConfig()
		    .withName("PMIStep")
			.withHadoopJarStep(hadoopJarStepFourth)
			.withActionOnFailure("TERMINATE_JOB_FLOW");

		RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
				.withName("Deddy-Dan-Super-Cluster")
				.withReleaseLabel("emr-4.3.0")
				.withLogUri("s3n://deddy-dan-dsps162/Project2/Logs")
				.withSteps(stepConfigFirst, stepConfigSec, stepConfigThird, stepConfigFourth)
				.withInstances(new JobFlowInstancesConfig()
						.withEc2KeyName("deddy-dan-keypair")
						.withInstanceCount(2)
						.withHadoopVersion("2.7.2")
						.withKeepJobFlowAliveWhenNoSteps(false)
						.withMasterInstanceType("m3.xlarge")
						.withSlaveInstanceType("m3.xlarge"));
		
		runFlowRequest.setServiceRole("EMR_DefaultRole");
		runFlowRequest.setJobFlowRole("EMR_EC2_DefaultRole");

		ClusterSummary theCluster = null;

		ListClustersRequest clustRequest = new ListClustersRequest().withClusterStates(ClusterState.WAITING);
		
		ListClustersResult clusterList = mapReduce.listClusters(clustRequest);
		for (ClusterSummary cluster : clusterList.getClusters()) {
		    if (cluster != null)
		        theCluster = cluster;
		}

		if (theCluster != null) {
			AddJobFlowStepsRequest request = new AddJobFlowStepsRequest()
					.withJobFlowId(theCluster.getId())
					.withSteps(stepConfigFirst);
			mapReduce.addJobFlowSteps(request);
			String jobFlowId = theCluster.getId();
			System.out.println("Ran job flow with id: " + jobFlowId);
		} 
		else {
			RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
			String jobFlowId = runJobFlowResult.getJobFlowId();
			System.out.println("Ran job flow with id: " + jobFlowId);
		}

   }

}