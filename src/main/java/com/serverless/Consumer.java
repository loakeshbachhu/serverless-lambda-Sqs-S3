package com.serverless;

import com.amazon.sqs.javamessaging.AmazonSQSExtendedClient;
import com.amazon.sqs.javamessaging.ExtendedClientConfiguration;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Consumer {

    private static final Logger LOG = LogManager.getLogger(Consumer.class);

  
    public void handleRequest(SQSEvent event) {

        event.getRecords().forEach(message -> {
            System.out.println(message.getBody());
        });

        // Print information about the message.
       /* for (Message message : messages) {
            System.out.println("\nMessage received.");
            System.out.println("  ID: " + message.getMessageId());
            System.out.println("  Receipt handle: " + message.getReceiptHandle());
            System.out.println("  Message body (first 200 characters): "
                    + message.getBody().substring(0, 200));
            System.out.println("  Size of the characters in the message: "
                    + message.getBody().length());
        }*/

    }


}
