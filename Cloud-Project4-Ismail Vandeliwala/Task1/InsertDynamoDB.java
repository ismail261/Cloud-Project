/*
 * Copyright 2012-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

/**
 * This sample demonstrates how to perform a few simple operations with the
 * Amazon DynamoDB service.
 */
public class InsertDynamoDB {

    /*
     * WANRNING:
     *      To avoid accidental leakage of your credentials, DO NOT keep
     *      the credentials file in your source directory.
     */

    static AmazonDynamoDBClient dynamoDB;
    static AmazonS3 s3;
    static LinkedList<String> list = new LinkedList<String>();

    /**
     * The only information needed to create a client are security credentials
     * consisting of the AWS Access Key ID and Secret Access Key. All other
     * configuration, such as the service endpoints, are performed
     * automatically. Client parameters, such as proxies, can be specified in an
     * optional ClientConfiguration object when constructing a client.
     *
     * @see com.amazonaws.auth.BasicAWSCredentials
     * @see com.amazonaws.auth.ProfilesConfigFile
     * @see com.amazonaws.ClientConfiguration
     */
    private static void init() throws Exception {
        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (C:\\Users\\Ismail\\.aws\\credentials).
         */
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (C:\\Users\\Ismail\\.aws\\credentials), and is in valid format.",
                    e);
        }
        dynamoDB = new AmazonDynamoDBClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        dynamoDB.setRegion(usWest2);
        
        s3 = new AmazonS3Client(credentials);
        s3.setRegion(usWest2);
    }

    public static void main(String[] args) throws Exception {
        init();

        try {
        	
        	/*
             * Download an object - When you download an object, you get all of
             * the object's metadata and a stream from which to read the contents.
             * It's important to read the contents of the stream as quickly as
             * possibly since the data is streamed directly from Amazon S3 and your
             * network connection will remain open until you read all the data or
             * close the input stream.
             *
             * GetObjectRequest also supports several other options, including
             * conditional downloading of objects based on modification times,
             * ETags, and selectively downloading a range of an object.
             */
        	String tableName = "comp-sys-act";

            // Create table if it does not exist yet
            if (Tables.doesTableExist(dynamoDB, tableName)) {
                System.out.println("Table " + tableName + " is already ACTIVE");
            } else {
                // Create a table with a primary hash key named 'name', which holds a string
                CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
                    .withKeySchema(new KeySchemaElement().withAttributeName("id").withKeyType(KeyType.HASH))
                    .withAttributeDefinitions(new AttributeDefinition().withAttributeName("id").withAttributeType(ScalarAttributeType.N))
                    .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(7L));
                    TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();
                System.out.println("Created Table: " + createdTableDescription);

                // Wait for it to become active
                System.out.println("Waiting for " + tableName + " to become ACTIVE...");
                Tables.waitForTableToBecomeActive(dynamoDB, tableName);
            }
            
            String bucketName = "cloud-project4";
            String key = "final_comp_act.csv";
            
            System.out.println("Downloading an object");
            S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
            
            int count = 0;
            BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
            String line = reader.readLine();
            while (true) {
            	line = reader.readLine();
            	if (line == null) break;
            	list.add(line);
            	count++;
            }
            System.out.println("count : "+count);
            
           for (int i = 0; i < list.size(); i++) {
        	   String a[] = list.get(i).split(",");
               Map<String, AttributeValue> item = newItem(i+1, a[0], a[1], a[2], a[3], a[4], a[5]);
               PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
               dynamoDB.putItem(putItemRequest);
           }
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to AWS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with AWS, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

    private static Map<String, AttributeValue> newItem(Integer id, String time, String lread, String lwrite, String scall, String sread, String swrite) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("id", new AttributeValue().withN(Integer.toString(id)));
        item.put("time", new AttributeValue(time));
        item.put("lread", new AttributeValue().withN(lread));
        item.put("lwrite", new AttributeValue().withN(lwrite));
        item.put("scall", new AttributeValue().withN(scall));
        item.put("sread", new AttributeValue().withN(sread));
        item.put("swrite", new AttributeValue().withN(swrite));

        return item;
    }
}