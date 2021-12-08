package emse;


import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import java.util.List;
import java.io.*;
import java.util.*;

public class RetrieveFileS3Bucket {
	
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
	
	public static List <Message> getMessage(SqsClient sqsClient, String queueName) {
		
		try {
			   String queueUrl = getQueueURL(sqsClient, queueName);
			   
			   ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
		                .queueUrl(queueUrl)
		                .maxNumberOfMessages(5)
		                .build();
		            List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
		            return messages;
	        } catch (SqsException e) {
	            System.err.println(e.awsErrorDetails().errorMessage());
	            System.exit(1);
	        }
		
	        return null;
	}
	
	public static void deleteMessages(SqsClient sqsClient, String queueName) {
        
			String queueUrl = getQueueURL(sqsClient, queueName);
        	List <Message> messages = getMessage(sqsClient, queueName);
        	
            for (Message message : messages) {
                DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build();
                sqsClient.deleteMessage(deleteMessageRequest);
            }
            
            System.out.println("Message Deleted");
		}
	
	public static BufferedReader getFileS3Bucket(S3Client s3Client, String bucketName, String fileName) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(fileName)
                .build();
        
        ResponseInputStream<GetObjectResponse> s3objectResponse = s3Client.getObject(getObjectRequest);
        
        BufferedReader buffReader = new BufferedReader(new InputStreamReader(s3objectResponse));
        
        
        return buffReader;
        
	}
	
	public static void getSum(S3Client s3Client, String bucketName, String fileName) {
		BufferedReader buffReader = getFileS3Bucket(s3Client, bucketName, fileName);
		
		           
		try {
			int sum = 0;
			String line; 
			while ((line = buffReader.readLine()) != null) {            
			    sum += Integer.parseInt(line);
			}
			System.out.println(sum);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	
	public static List<Integer> transfromFileToList(BufferedReader buffReader) {
		
		List<Integer> result = new ArrayList<>();
		
		try {
			String line;
			while ((line = buffReader.readLine()) != null) {
				result.add(Integer.parseInt(line));
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
		
	}
	
	public static void minmaxValue(S3Client s3Client, String bucketName, String fileName) {
		BufferedReader buffReader = getFileS3Bucket(s3Client, bucketName, fileName);
		
		List<Integer> buffList = transfromFileToList(buffReader);
		List<Integer> sortedBuffList = new ArrayList<>(buffList);
		Collections.sort(sortedBuffList);
		
		System.out.println(sortedBuffList.get(0));
		System.out.println(sortedBuffList.get(sortedBuffList.size() - 1 ));
	}
	
	public static void deleteBucketContent(S3Client s3Client, String bucketName, String fileName) {
        
		DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(fileName)
                .build();
        
        s3Client.deleteObject(deleteObjectRequest);
	}
	
	public static void deleteS3Bucket(S3Client s3Client, String bucketName) {
		 DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucketName).build();
	        s3Client.deleteBucket(deleteBucketRequest);
	        s3Client.close();
        System.out.println("Done!");
	}
	
	public static void main(String [] args) {
		
		Region region = Region.US_EAST_2;
		String queueName = "S3QUEUE";
		List <String> names  = new ArrayList<String>();

		S3Client s3Client = S3Client.builder().region(region).build();
		SqsClient sqsClient = SqsClient.builder().region(region).build();
		
		List <Message> messages = getMessage(sqsClient, queueName);
		
		
		// create a list of strings from a list of messages
		// names list contains the s3bucket name and the file name
		for(Message message : messages) {
			names.add(message.body().toString());	
		}
		
		// Not needed: display messages
		names.forEach(it->System.out.println(it));
		
		
		deleteMessages(sqsClient, queueName);
		
		getFileS3Bucket(s3Client, names.get(0), names.get(1));
		
		getSum(s3Client, names.get(0), names.get(1));
		minmaxValue(s3Client, names.get(0), names.get(1));
		
		deleteBucketContent(s3Client, names.get(0), names.get(1));
		deleteS3Bucket(s3Client, names.get(0));
		
	}
	
}
