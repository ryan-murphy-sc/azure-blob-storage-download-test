package com.example.storage;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobServiceClientBuilder;

public class Downloader {

	private static final Logger LOGGER = LoggerFactory.getLogger(Downloader.class);

	public static void main(String[] args) {
		if (args.length < 4) {
			System.err.println("Expected 4 args. <account> <key> <container> <blob>");
			System.exit(1);
		}
		String accountName = args[0];
		String accountKey = args[1];
		String containerName = args[2];
		String blobName = args[3];

		System.out.println("Account Name: " + accountName);
		System.out.println("Container Name: " + containerName);
		System.out.println("Blob Name: " + blobName);

		final BlobAsyncClient blobAsyncClient = new BlobServiceClientBuilder()
				.connectionString(buildConnectionString(accountName, accountKey))
				.buildAsyncClient()
				.getBlobContainerAsyncClient(containerName)
				.getBlobAsyncClient(blobName);

		blobAsyncClient
				.getProperties()
				.map(bp -> {
					System.out.println("-----Get Properties Blob Size: " + bp.getBlobSize());
					return bp;
				})
				.flatMap(n -> blobAsyncClient.downloadToFile(blobName, true))
				.doOnSuccess(bp -> {
					LOGGER.info("File downloaded");
					System.out.println("-----File downloaded-----");
					System.out.println("---Download reported blob size: " + bp.getBlobSize());
					System.out.println("---File size:" + new File(blobName).length());
				})
				.doOnError(Throwable::printStackTrace)
				.block();
	}

	private static String buildConnectionString(String accountName, String accountKey) {
		return "DefaultEndpointsProtocol=https;AccountName=" + accountName + ";AccountKey=" + accountKey;
	}

}
