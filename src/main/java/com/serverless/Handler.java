package com.serverless;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;


import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import com.amazon.sqs.javamessaging.AmazonSQSExtendedClient;
import com.amazon.sqs.javamessaging.ExtendedClientConfiguration;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;

public class Handler implements RequestHandler<Map<String, Object>, ApiGatewayResponse> {

    private static final Logger LOG = LogManager.getLogger(Handler.class);

    private final static String S3_BUCKET_NAME = UUID.randomUUID() + "-"
            + DateTimeFormat.forPattern("yyMMdd-hhmmss").print(new DateTime());

    private final static String SQS_QUEUE_URL =
         "https://sqs.us-east-2.amazonaws.com/121279407446/lambda_Sqs_Large_Objects"; //Personal Account

    @Override
    public ApiGatewayResponse handleRequest(Map<String, Object> input, Context context) {

        String json = null;
//		int stringLength = 240;
//		char[] chars = new char[stringLength];
//		Arrays.fill(chars, 'x');
//		json = new String(chars);


        String file = "src/main/resources/myJsonFile.json";
        try {
            json = new String(Files.readAllBytes(Paths.get(file)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<Message> messages = null;

        Integer objectSize = getObjectSize(Collections.singletonList(json));
        if (objectSize > 262144) {
            System.out.println("The size of the object is greater than 256 KB " + objectSize+ " bytes");
            messages = getMessagesUsingExtendedLibrary(json);
        } else {
            System.out.println("The size of the object is less than 256 KB " + objectSize+ " bytes");
            messages = getMessages(json);
        }


        // Print information about the message.
        for (Message message : messages) {
            System.out.println("\nMessage received.");
            System.out.println("  ID: " + message.getMessageId());
            System.out.println("  Receipt handle: " + message.getReceiptHandle());
            System.out.println("  Message body (first 200 characters): "
                    + message.getBody().substring(0, 200));
            System.out.println("  Size of the characters in the message: "
                    + message.getBody().length());
        }


        try {
            return ApiGatewayResponse.builder()
                    .setStatusCode(200)
                    .setObjectBody(new ObjectMapper().writeValueAsString("Success"))
                    .setHeaders(Collections.singletonMap("X-Powered-By", "AWS Lambda & serverless"))
                    .build();
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    private List<Message> getMessagesUsingExtendedLibrary(String json) {
        final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        final BucketLifecycleConfiguration.Rule expirationRule = new BucketLifecycleConfiguration.Rule();
        expirationRule.withExpirationInDays(14).withStatus("Enabled");
        final BucketLifecycleConfiguration lifecycleConfig =
                new BucketLifecycleConfiguration().withRules(expirationRule);

        // Create the bucket and allow message objects to be stored in the bucket.
        s3.createBucket(S3_BUCKET_NAME);
        s3.setBucketLifecycleConfiguration(S3_BUCKET_NAME, lifecycleConfig);
        System.out.println("Bucket created and configured.");

        final ExtendedClientConfiguration extendedClientConfig =
                new ExtendedClientConfiguration()
                        .withLargePayloadSupportEnabled(s3, S3_BUCKET_NAME);

        final AmazonSQS sqsExtended = new AmazonSQSExtendedClient(AmazonSQSClientBuilder
                .defaultClient(), extendedClientConfig);


        // Send the message.
        final SendMessageRequest myMessageRequest =
                new SendMessageRequest(SQS_QUEUE_URL, json);
        sqsExtended.sendMessage(myMessageRequest);
        System.out.println("Sent the message.");

        // Receive the message.
        final ReceiveMessageRequest receiveMessageRequest =
                new ReceiveMessageRequest(SQS_QUEUE_URL);
        List<Message> messages = sqsExtended
                .receiveMessage(receiveMessageRequest).getMessages();
        System.out.println("Receieved the message.");

        if (messages != null && messages.size() > 0) {
            //		 Delete the message, the queue, and the bucket.
            final String messageReceiptHandle = messages.get(0).getReceiptHandle();
            sqsExtended.deleteMessage(new DeleteMessageRequest(SQS_QUEUE_URL,
                    messageReceiptHandle));
            System.out.println("Deleted the message.");
            deleteBucketAndAllContents(s3);
            System.out.println("Deleted the bucket.");
        }
        return messages;
    }

    private List<Message> getMessages(String json){
        final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
        SendMessageRequest send_msg_request = new SendMessageRequest()
                .withQueueUrl(SQS_QUEUE_URL)
                .withMessageBody(json);
        sqs.sendMessage(send_msg_request);
        System.out.println("Sent the message.");

        List<Message> messages = sqs.receiveMessage(SQS_QUEUE_URL).getMessages();
        System.out.println("Receieved the message.");
        return messages;
    }

    private static void deleteBucketAndAllContents(AmazonS3 client) {

        ObjectListing objectListing = client.listObjects(S3_BUCKET_NAME);

        while (true) {
            for (S3ObjectSummary objectSummary : objectListing
                    .getObjectSummaries()) {
                client.deleteObject(S3_BUCKET_NAME, objectSummary.getKey());
            }

            if (objectListing.isTruncated()) {
                objectListing = client.listNextBatchOfObjects(objectListing);
            } else {
                break;
            }
        }

        final VersionListing list = client.listVersions(
                new ListVersionsRequest().withBucketName(S3_BUCKET_NAME));

        for (S3VersionSummary s : list.getVersionSummaries()) {
            client.deleteVersion(S3_BUCKET_NAME, s.getKey(), s.getVersionId());
        }

        client.deleteBucket(S3_BUCKET_NAME);
    }

    private static int getObjectSize(List object) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            oos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return baos.size();
    }
}
