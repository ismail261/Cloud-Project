/*
 * Copyright 2010-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import java.io.File;
import java.io.IOException;

import weka.core.Instances;
import weka.core.converters.ArffSaver;
import weka.core.converters.CSVLoader;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;

/**
 * This sample demonstrates how to make basic requests to Amazon S3 using the
 * AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web Services developer
 * account, and be signed up to use Amazon S3. For more information on Amazon
 * S3, see http://aws.amazon.com/s3.
 * <p>
 * WANRNING:</b> To avoid accidental leakage of your credentials, DO NOT keep
 * the credentials file in your source directory.
 *
 * http://aws.amazon.com/security-credentials
 */
public class UploadS3 {

    public static void main(String[] args) throws IOException {

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

        AmazonS3 s3 = new AmazonS3Client(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        s3.setRegion(usWest2);

        String bucketName = "cloud-project4";
        
        String key = "final_comp_act.csv";
        String key1 = "final_comp_act.arff";

        System.out.println("===========================================");
        System.out.println("Getting Started with Amazon S3");
        System.out.println("===========================================\n");

        try {
            /*
             * Create a new S3 bucket - Amazon S3 bucket names are globally unique,
             * so once a bucket name has been taken by any user, you can't create
             * another bucket with that same name.
             *
             * You can optionally specify a location for your bucket if you want to
             * keep your data closer to your applications or users.
             */
            System.out.println("Creating bucket " + bucketName + "\n");
            s3.createBucket(bucketName);

            /*
             * Upload an object to your bucket - You can easily upload a file to
             * S3, or upload directly an InputStream if you know the length of
             * the data in the stream. You can also specify your own metadata
             * when uploading to S3, which allows you set a variety of options
             * like content-type and content-encoding, plus additional metadata
             * specific to your applications.
             */
            String csv = "D:\\books\\semester4\\Cloud_Computing\\Project4-Weka-Graph\\"+key;
    		String output = "D:\\books\\semester4\\Cloud_Computing\\Project4-Weka-Graph\\"+key1;
    	    CSVLoader loader = new CSVLoader();
    	    loader.setSource(new File(csv));
    	    Instances data = loader.getDataSet();
    	 
    	    // save ARFF
    	    ArffSaver saver = new ArffSaver();
    	    saver.setInstances(data);
    	    saver.setFile(new File(output));
    	    saver.writeBatch();
    	    
            File file = new File(csv);
            s3.putObject(new PutObjectRequest(bucketName, key, file));
            
            file = new File(output);
            s3.putObject(new PutObjectRequest(bucketName, key1, file));

            
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }
}
