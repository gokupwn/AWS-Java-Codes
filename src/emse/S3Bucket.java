package emse;


import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import java.util.List;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.io.IOException;

public class S3Bucket {
	
	public static void createBucket( S3Client s3Client, String bucketName) {

        try {
            S3Waiter s3Waiter = s3Client.waiter();
             CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            s3Client.createBucket(bucketRequest);
            HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build();


            // Wait until the bucket is created and print out the response
            WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
            waiterResponse.matched().response().ifPresent(System.out::println);
            System.out.println(bucketName +" is ready");

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }
	
	public static void uploadFileS3Bucket(S3Client s3, String bucketName,String PATH) {
		  try
	        {
	            RandomAccessFile aFile = new RandomAccessFile(PATH,"r");
	 
	            FileChannel inChannel = aFile.getChannel();
	            long fileSize = inChannel.size();
	 
	            ByteBuffer buffer = ByteBuffer.allocate((int) fileSize);
	            inChannel.read(buffer);
	            buffer.flip();
	            
	          
	    	    PutObjectRequest objectRequest = PutObjectRequest.builder()
	                    .bucket(bucketName)
	                    .key(PATH.substring(PATH.lastIndexOf("\\")+1))
	                    .build();
	    	    
	            s3.putObject(objectRequest, RequestBody.fromByteBuffer(buffer));
	    	    
	           
	            inChannel.close();
	            aFile.close();
	            
	            System.out.printf("%s file uploaded to Bucket %s ", PATH, bucketName);
	            
	        } catch (IOException exc){
	            System.out.println(exc);
	            System.exit(1);
	        }
	}
	
	public static void createQueue(SqsClient sqsClient, String queueName) {
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();

            sqsClient.createQueue(createQueueRequest);
            
            System.out.printf("Queue %s created\n", queueName);
	}
	
	public static String getQueueURL(SqsClient sqsClient, String queueName) {
		
		try {
			
			GetQueueUrlResponse getQueueUrlResponse = sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            String queueUrl = getQueueUrlResponse.queueUrl();
            return queueUrl;

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
	}
	
	public  static void sendMessage(SqsClient sqsClient, String queueName, String fileName, String bucketName) {
		
		String queueUrl = getQueueURL(sqsClient, queueName);
		
        SendMessageBatchRequest sendMessageBatchRequest = SendMessageBatchRequest.builder()
                .queueUrl(queueUrl)
                .entries(SendMessageBatchRequestEntry.builder().id("id1").messageBody(bucketName).build(),
                        SendMessageBatchRequestEntry.builder().id("id2").messageBody(fileName.substring(fileName.lastIndexOf("\\")+1)).delaySeconds(10).build())
                .build();
        
            sqsClient.sendMessageBatch(sendMessageBatchRequest);
            
            System.out.printf("Message added to queue %s", queueName);
	}
	
	public static void main(String[] args) {
		
		/* File to upload*/
		String PATH = "src\\emse\\values.csv";
		
	    Region region = Region.US_EAST_2;
	    S3Client s3 = S3Client.builder().region(region).build();
	    
	    createBucket(s3, "newbucket1337110");
	    
	    uploadFileS3Bucket(s3, "newbucket1337110", PATH);
	    
	    SqsClient sqsClient = SqsClient.builder().region(region).build(); 
	    
	    createQueue(sqsClient, "S3QUEUE");
	    
	    sendMessage(sqsClient, "S3QUEUE", PATH, "newbucket1337110" );
	    
      
        
	}
}
