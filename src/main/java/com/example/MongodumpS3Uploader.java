package com.example;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.ZoneId;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import org.apache.commons.lang3.time.DateUtils;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

public class MongodumpS3Uploader {
    private static final int PART_SIZE = 5 * 1024 * 1024;  // 5 MB part size

    // Method to execute the mongodump command and return an InputStream
    private static InputStream runMongoDump() throws IOException {
        // Retrieve the mongodump command from the environment variable
        String mongodumpCommand = System.getenv("MONGODUMP_COMMAND");

        if (mongodumpCommand == null || mongodumpCommand.isEmpty()) {
            throw new IllegalArgumentException("MONGODUMP_COMMAND environment variable is not set");
        }

        // Split the command into a list of arguments for ProcessBuilder
        String[] commandParts = mongodumpCommand.split(" ");

        // Create the ProcessBuilder with the command
        final ProcessBuilder processBuilder = new ProcessBuilder(commandParts);
        processBuilder.redirectErrorStream(true);  // Redirect error stream to the input stream
        final Process process = processBuilder.start();
        return process.getInputStream();
    }

    // Method to upload the data from the mongodump input stream to S3 using multipart upload
    public static void multipartUploadWithS3Client(S3Client s3Client, String bucketName, String key, InputStream inputStream) throws IOException {

        // Step 1: Initiate the multipart upload
        final CreateMultipartUploadResponse createMultipartUploadResponse = s3Client.createMultipartUpload(CreateMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build());
        final String uploadId = createMultipartUploadResponse.uploadId();

        // Step 2: TODO: Determine the optimal part size based on the total data size
        final int partSize = PART_SIZE;
        System.out.println("Using part size: " + partSize + " bytes");

        // Prepare to upload the file in parts
        final List<CompletedPart> completedParts = new ArrayList<>();
        byte[] buffer = new byte[partSize];  // Buffer to hold data for each part
        int partNumber = 1;
        int bytesRead;
        int currentBufferSize = 0;
        long totalBytesUploaded = 0;

        try {
            while ((bytesRead = inputStream.read(buffer, currentBufferSize, buffer.length - currentBufferSize)) != -1) {
                currentBufferSize += bytesRead;

                // Only upload the part when the buffer reaches or exceeds the part size
                if (currentBufferSize >= partSize) {
                    uploadPart(s3Client, bucketName, key, uploadId, buffer, partNumber, currentBufferSize);
                    totalBytesUploaded += currentBufferSize;
                    currentBufferSize = 0; // Reset buffer after part upload
                    partNumber++;
                }
            }

            // Handle the final part if it didn't reach the full part size
            if (currentBufferSize > 0) {
                uploadPart(s3Client, bucketName, key, uploadId, buffer, partNumber, currentBufferSize);
                totalBytesUploaded += currentBufferSize;
                partNumber++;
            }

        } catch (IOException e) {
            System.err.println("Error reading input stream or uploading parts: " + e.getMessage());
            abortMultipartUpload(s3Client, bucketName, key, uploadId);
            throw e;
        }

        // Step 3: Complete the multipart upload
        final CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(key)
                .uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
                .build();

        try {
            s3Client.completeMultipartUpload(completeRequest);
            System.out.println("Upload completed successfully with total bytes: " + totalBytesUploaded);
        } catch (S3Exception e) {
            System.err.println("Error completing multipart upload: " + e.awsErrorDetails().errorMessage());
            abortMultipartUpload(s3Client, bucketName, key, uploadId);
            throw e;
        }
    }

    // Helper method to upload part
    private static void uploadPart(S3Client s3Client, String bucketName, String key, String uploadId, byte[] buffer, int partNumber, int bufferSize) {
        final ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, 0, bufferSize);  // Wrap the buffer for the part

        final UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                .bucket(bucketName)
                .key(key)
                .uploadId(uploadId)
                .partNumber(partNumber)
                .contentLength((long) bufferSize)
                .build();

        final UploadPartResponse uploadPartResponse = s3Client.uploadPart(uploadPartRequest, RequestBody.fromByteBuffer(byteBuffer));

        final CompletedPart part = CompletedPart.builder()
                .partNumber(partNumber)
                .eTag(uploadPartResponse.eTag())
                .build();
        System.out.println("Uploaded part " + partNumber + " (size: " + bufferSize + " bytes)");
    }

    // Helper method to abort a multipart upload in case of failure
    private static void abortMultipartUpload(S3Client s3Client, String bucketName, String key, String uploadId) {
        s3Client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(key)
                .uploadId(uploadId)
                .build());
        System.out.println("Multipart upload aborted.");
    }

    public static void main(String[] args) {
        final S3Client s3Client = S3Client.builder()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .region(Region.US_EAST_1)
                .build();

        String bucketName = System.getenv("S3_BUCKET_NAME"); // Load from environment variable
        if (bucketName == null) {
            System.err.println("Error: S3_BUCKET_NAME environment variable is not set.");
            return;
        }

        // Get the current timestamp
        final Instant now = Instant.now();
        
        // Format the timestamp as a string (e.g., "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        final String formattedTimestamp = DateTimeFormatter
            .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            .withZone(ZoneId.of("UTC"))
            .format(now);

        try {
            final InputStream dumpInputStream = runMongoDump();
            // use timestamp as S3 object key
            multipartUploadWithS3Client(s3Client, bucketName, formattedTimestamp, dumpInputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
