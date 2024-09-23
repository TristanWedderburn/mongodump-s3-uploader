package com.example;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.ZoneId;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
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

        long startUploadTime = System.nanoTime(); // Start timer for entire upload

        // Step 1: Initiate the multipart upload
        final CreateMultipartUploadResponse createMultipartUploadResponse = s3Client.createMultipartUpload(CreateMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build());
        final String uploadId = createMultipartUploadResponse.uploadId();

        // Prepare to upload the file in parts
        final List<CompletedPart> completedParts = new ArrayList<>();
        byte[] buffer = new byte[PART_SIZE];  // Buffer to hold data for each part
        int partNumber = 1;
        int bytesRead;
        int currentBufferSize = 0;
        long totalBytesUploaded = 0;

        // For perf. analysis
        int readCount = 0;
        int uploadCount = 0;
        long totalReadTime = 0;
        long totalUploadTime = 0;

        try {
            while (true) {
                // Measure time for reading from InputStream
                long startReadTime = System.nanoTime();
                bytesRead = inputStream.read(buffer, currentBufferSize, buffer.length - currentBufferSize);
                long endReadTime = System.nanoTime();
                totalReadTime += (endReadTime - startReadTime);
                readCount++;

                if (bytesRead == -1) {
                    break; // End of stream
                }

                currentBufferSize += bytesRead;

                // Only upload the part when the buffer reaches or exceeds the part size
                if (currentBufferSize >= PART_SIZE) {
                    long startUploadPartTime = System.nanoTime();  // Start timer for each part upload
                    uploadPart(s3Client, bucketName, key, uploadId, buffer, partNumber, currentBufferSize, completedParts);
                    long endUploadPartTime = System.nanoTime();  // End timer for each part upload

                    totalUploadTime += (endUploadPartTime - startUploadPartTime);
                    uploadCount++;

                    totalBytesUploaded += currentBufferSize;
                    currentBufferSize = 0; // Reset buffer after part upload
                    partNumber++;
                }
            }

            // Handle the final part if it didn't reach the full part size
            if (currentBufferSize > 0) {
                long startUploadPartTime = System.nanoTime();  // Start timer for final part
                uploadPart(s3Client, bucketName, key, uploadId, buffer, partNumber, currentBufferSize, completedParts);
                long endUploadPartTime = System.nanoTime();  // End timer for final part

                totalUploadTime += (endUploadPartTime - startUploadPartTime);
                uploadCount++;

                totalBytesUploaded += currentBufferSize;
                partNumber++;
            }

            long endUploadTime = System.nanoTime();  // End timer for entire upload

            System.out.println("Total data uploaded: " + totalBytesUploaded + " bytes");
            System.out.println("Average time spent reading from InputStream: " + (totalReadTime / readCount) / 1e6 + " ms (for " + readCount + " reads)");
            System.out.println("Average time spent uploading parts: " + (totalUploadTime / uploadCount) / 1e6 + " ms (for " + uploadCount + " uploads)");
            System.out.println("Total time for the entire upload process: " + (endUploadTime - startUploadTime) / 1e6 + " ms");

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
    private static void uploadPart(S3Client s3Client, String bucketName, String key, String uploadId, byte[] buffer, int partNumber, int bufferSize, List<CompletedPart> completedParts) {
        final ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, 0, bufferSize);  // Wrap the buffer for the part

        final UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                .bucket(bucketName)
                .key(key)
                .uploadId(uploadId)
                .partNumber(partNumber)
                .contentLength((long) bufferSize)
                .build();

        final UploadPartResponse uploadPartResponse = s3Client.uploadPart(uploadPartRequest, RequestBody.fromByteBuffer(byteBuffer));

        final CompletedPart completedPart = CompletedPart.builder()
                .partNumber(partNumber)
                .eTag(uploadPartResponse.eTag())
                .build();
        completedParts.add(completedPart);
        System.out.println("Uploaded part " + partNumber + " (size: " + bufferSize + " bytes) with eTag: " + uploadPartResponse.eTag());
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

        System.out.println("S3 object key: " + formattedTimestamp);

        try {
            final InputStream dumpInputStream = runMongoDump();
            // use timestamp as S3 object key
            multipartUploadWithS3Client(s3Client, bucketName, formattedTimestamp, dumpInputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
