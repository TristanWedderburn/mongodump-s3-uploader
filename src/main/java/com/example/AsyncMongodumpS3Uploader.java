package com.example;

import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;

import java.io.*;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.time.Duration;

public class AsyncMongodumpS3Uploader {
    private static final int PART_SIZE = 5 * 1024 * 1024;  // 5 MB part size

    // Method to execute the mongodump command and return an InputStream
    private static InputStream runMongoDump() throws IOException {
        String mongodumpCommand = System.getenv("MONGODUMP_COMMAND");
        if (mongodumpCommand == null || mongodumpCommand.isEmpty()) {
            throw new IllegalArgumentException("MONGODUMP_COMMAND environment variable is not set");
        }

        String[] commandParts = mongodumpCommand.split(" ");
        ProcessBuilder processBuilder = new ProcessBuilder(commandParts);
        processBuilder.redirectErrorStream(true);
        return processBuilder.start().getInputStream();
    }

    // Non-blocking async multipart upload
    public static void multipartUploadWithS3AsyncClient(S3AsyncClient s3AsyncClient, String bucketName, String key, InputStream inputStream) throws IOException, InterruptedException, ExecutionException {

        // Step 1: Initiate the multipart upload
        CompletableFuture<CreateMultipartUploadResponse> createMultipartUploadResponseFuture =
                s3AsyncClient.createMultipartUpload(CreateMultipartUploadRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .build());

        CreateMultipartUploadResponse createMultipartUploadResponse = createMultipartUploadResponseFuture.join();
        String uploadId = createMultipartUploadResponse.uploadId();

        // A thread-safe queue to store upload results
        List<CompletedPart> completedParts = Collections.synchronizedList(new ArrayList<>());
        byte[] buffer = new byte[PART_SIZE];
        int partNumber = 1;
        int bytesRead;
        int currentBufferSize = 0;
        long totalBytesUploaded = 0;
        int readCount = 0;
        int uploadCount = 0;
        long totalReadTime = 0;
        long totalUploadTime = 0;

        List<CompletableFuture<CompletedPart>> uploadFutures = new ArrayList<>();

        try {
            // Step 2: Read from input stream and schedule uploads
            while (true) {
                long startReadTime = System.nanoTime();
                bytesRead = inputStream.read(buffer, currentBufferSize, buffer.length - currentBufferSize);
                long endReadTime = System.nanoTime();
                totalReadTime += (endReadTime - startReadTime);
                readCount++;

                if (bytesRead == -1) {
                    break;
                }

                currentBufferSize += bytesRead;

                if (currentBufferSize >= PART_SIZE) {
                    byte[] partData = Arrays.copyOf(buffer, currentBufferSize);  // Copy buffer to avoid overwriting it during upload
                    CompletableFuture<CompletedPart> uploadFuture = uploadPartAsync(s3AsyncClient, bucketName, key, uploadId, partData, partNumber);
                    uploadFutures.add(uploadFuture);
                    partNumber++;
                    currentBufferSize = 0;
                }
            }

            // Handle the final part if it didn't reach the full part size
            if (currentBufferSize > 0) {
                byte[] partData = Arrays.copyOf(buffer, currentBufferSize);  // Copy buffer for final part
                CompletableFuture<CompletedPart> uploadFuture = uploadPartAsync(s3AsyncClient, bucketName, key, uploadId, partData, partNumber);
                uploadFutures.add(uploadFuture);
            }

        } catch (IOException e) {
            System.err.println("Error reading input stream or uploading parts: " + e.getMessage());
            abortMultipartUploadAsync(s3AsyncClient, bucketName, key, uploadId);
            throw e;
        }

        // Wait for all uploads to complete
        CompletableFuture.allOf(uploadFutures.toArray(new CompletableFuture[0])).join();

        // Sort the completed parts to ensure order
        uploadFutures.forEach(future -> {
            try {
                completedParts.add(future.get());
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Failed to retrieve upload future", e);
            }
        });

        // Step 3: Complete the multipart upload
        completedParts.sort(Comparator.comparing(CompletedPart::partNumber));

        CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(key)
                .uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
                .build();

        s3AsyncClient.completeMultipartUpload(completeRequest).join();
        System.out.println("Upload completed successfully with total bytes: " + totalBytesUploaded);

        System.out.println("Average read time: " + (totalReadTime / readCount) / 1e6 + " ms (for " + readCount + " reads)");
        System.out.println("Average upload time: " + (totalUploadTime / uploadCount) / 1e6 + " ms (for " + uploadCount + " uploads)");
    }

    // Helper method for async part upload
    private static CompletableFuture<CompletedPart> uploadPartAsync(S3AsyncClient s3AsyncClient, String bucketName, String key, String uploadId, byte[] partData, int partNumber) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(partData);

        UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                .bucket(bucketName)
                .key(key)
                .uploadId(uploadId)
                .partNumber(partNumber)
                .contentLength((long) partData.length)
                .build();

        CompletableFuture<UploadPartResponse> uploadPartResponseFuture = s3AsyncClient.uploadPart(uploadPartRequest, AsyncRequestBody.fromByteBuffer(byteBuffer));

        return uploadPartResponseFuture.thenApply(uploadPartResponse -> {
            System.out.println("Uploaded part " + partNumber + " (size: " + partData.length + " bytes)");
            return CompletedPart.builder()
                    .partNumber(partNumber)
                    .eTag(uploadPartResponse.eTag())
                    .build();
        });
    }

    // Async helper method to abort multipart upload in case of failure
    private static void abortMultipartUploadAsync(S3AsyncClient s3AsyncClient, String bucketName, String key, String uploadId) {
        s3AsyncClient.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(key)
                .uploadId(uploadId)
                .build()).join();
        System.out.println("Multipart upload aborted.");
    }

    public static void main(String[] args) {
        SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.builder()
                .maxConcurrency(50)  // Adjust based on your system's load
                .connectionAcquisitionTimeout(Duration.ofSeconds(90))  // Set acquisition timeout
                .build();
        // Step 2: Initialize the S3AsyncClient with the custom httpClient
        S3AsyncClient s3AsyncClient = S3AsyncClient.builder()
                 // Step 1: Create a custom NettyNioAsyncHttpClient with defined timeouts
                .httpClient(httpClient)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .region(Region.US_EAST_1)
                .build();

        String bucketName = System.getenv("S3_BUCKET_NAME");  // Load bucket name from environment variable
        if (bucketName == null) {
            System.err.println("Error: S3_BUCKET_NAME environment variable is not set.");
            return;
        }

        // Get the current timestamp for the S3 object key
        Instant now = Instant.now();
        String formattedTimestamp = DateTimeFormatter
                .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
                .withZone(ZoneId.of("UTC"))
                .format(now);

        System.out.println("S3 object key: " + formattedTimestamp);

        try {
            InputStream dumpInputStream = runMongoDump();

            // Step 3: Use the multipart upload function with S3AsyncClient
            multipartUploadWithS3AsyncClient(s3AsyncClient, bucketName, formattedTimestamp, dumpInputStream);

        } catch (IOException | ExecutionException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            // Step 4: Close the S3AsyncClient and Netty HTTP client
            s3AsyncClient.close();
            httpClient.close();
        }
    }
}
