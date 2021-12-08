package emse;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;

public class EC2Worker {
	
	public static String getQueueURL(SqsClient sqsClient, String queueName) {
		
		try {
			
			GetQueueUrlResponse getQueueUrlResponse = sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            String queueUrl = getQueueUrlResponse.queueUrl();
            return queueUrl;

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        	 return "";
        }
       
	}
	
	public static boolean createQueue(SqsClient sqsClient, String queueName) {
		if (getQueueURL(sqsClient, queueName) == "") {
	        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
	                .queueName(queueName)
	                .build();
	
	            sqsClient.createQueue(createQueueRequest);
	            
	            System.out.printf("Queue %s created\n", queueName);
	            return true;
		}
		else {
			return false;
		}
	}
	
	
	public static void main(String[] args) {
		
		Region region = Region.US_EAST_2;

		S3Client s3Client = S3Client.builder().region(region).build();
		SqsClient sqsClient = SqsClient.builder().region(region).build();
		
		if(!createQueue(sqsClient, "Inbox")) {
			System.out.println("Already Created Inbox");
		}
		
		if(!createQueue(sqsClient, "Outbox")) {
			System.out.println("Already Created outbox");
		}

	}
	
	
}
