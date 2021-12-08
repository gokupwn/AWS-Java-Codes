package emse;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.StartInstancesRequest;
import software.amazon.awssdk.services.ec2.model.StopInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.Reservation;

public class EC2Instance {
	
	   public static String createEC2Instance(Ec2Client ec2,String name, String amiId ) {

	        RunInstancesRequest runRequest = RunInstancesRequest.builder()
	                .imageId(amiId)
	                .instanceType(InstanceType.T2_MICRO)
	                .maxCount(1)
	                .minCount(1)
	                .build();

	        RunInstancesResponse response = ec2.runInstances(runRequest);
	        String instanceId = response.instances().get(0).instanceId();

	        Tag tag = Tag.builder()
	                .key("Name")
	                .value(name)
	                .build();

	        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
	                .resources(instanceId)
	                .tags(tag)
	                .build();

	        try {
	            ec2.createTags(tagRequest);
	            System.out.printf(
	                    "Successfully started EC2 Instance %s based on AMI %s",
	                    instanceId, amiId);

	          return instanceId;

	        } catch (Ec2Exception e) {
	            System.err.println(e.awsErrorDetails().errorMessage());
	            System.exit(1);
	        }

	        return "";
	    }
	   
	   public static void startInstance(Ec2Client ec2, String instanceId) {

	        StartInstancesRequest request = StartInstancesRequest.builder()
	                .instanceIds(instanceId)
	                .build();

	        ec2.startInstances(request);
	        System.out.printf("Successfully started instance %s", instanceId);
	    }
	   
	   public static void stopInstance(Ec2Client ec2, String instanceId) {

	        StopInstancesRequest request = StopInstancesRequest.builder()
	                .instanceIds(instanceId)
	                .build();

	        ec2.stopInstances(request);
	        System.out.printf("Successfully stopped instance %s", instanceId);
	    }
	   
	   public static String getEC2InstanceStatus( Ec2Client ec2, String instanceId){
	        String nextToken = null;

	        try {

	            do {
	                DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(6).nextToken(nextToken).build();
	                DescribeInstancesResponse response = ec2.describeInstances(request);

	                for (Reservation reservation : response.reservations()) {
	                    for (Instance instance : reservation.instances()) {
	                    	
	                        if ( instanceId.equals(instance.instanceId())) {
	                        	return instance.state().name().toString();
	                        }
	                }
	            }
	                nextToken = response.nextToken();
	            } while (nextToken != null);

	        } catch (Ec2Exception e) {
	            System.err.println(e.awsErrorDetails().errorMessage());
	            System.exit(1);
	        }
	        
	        return "";
	   }
	   
	   public static void switchStatus(Ec2Client ec2, String instanceId){
		   
		   String currentStatus = getEC2InstanceStatus(ec2, instanceId);
		   
		   if (currentStatus.equals("running")) {
			   stopInstance(ec2, instanceId);
		   }
		   else if (currentStatus.equals("stooped")) {
			   startInstance(ec2, instanceId);
		   } 
		   
	   }
	   
	   
	   public static void main(String[] args) {
		   
		   Region region = Region.US_EAST_2;
		   Ec2Client ec2 = Ec2Client.builder().region(region).build();
		   String name = "ec2Java";
		   String amiId = "ami-0d718c3d715cec4a7";
		   
		   String instanceId = createEC2Instance(ec2,name, amiId );
		   switchStatus(ec2, instanceId);
	   }

}
