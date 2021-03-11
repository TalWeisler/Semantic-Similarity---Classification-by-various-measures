import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.apache.log4j.BasicConfigurator;

public class Ass3 {
    public static void main(String[] args) {
        BasicConfigurator.configure();
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClient.builder().withRegion(Regions.US_EAST_1).build();
        //"s3n://assignment3dsp/biarcs/ ->all the cprpus   ,  s3n://ass03/text.txt -> our example , s3n://assignment3dsp/biarcs/biarcs.00-of-99 -> 1 file"
        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3n://ass03/DSP3.jar")
                .withMainClass("Word2Vector")
                .withArgs("s3n://assignment3dsp/biarcs/",
                        "s3n://ass03/Step4");

        StepConfig stepConfig = new StepConfig()
                .withName("word2vector")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(6)
                .withMasterInstanceType(InstanceType.M4Xlarge.toString())
                .withSlaveInstanceType(InstanceType.M4Xlarge.toString())
                .withHadoopVersion("3.2.1")
                .withEc2KeyName("Ass1_key")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("DSP3")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withLogUri("s3n://ass03/Log/")
                .withReleaseLabel("emr-6.2.0");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
