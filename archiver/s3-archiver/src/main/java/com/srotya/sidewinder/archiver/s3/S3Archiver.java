/**
 * Copyright 2017 Ambud Sharma
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.srotya.sidewinder.archiver.s3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.srotya.sidewinder.core.storage.gorilla.ArchiveException;
import com.srotya.sidewinder.core.storage.gorilla.Archiver;
import com.srotya.sidewinder.core.storage.gorilla.archival.DiskArchiver;
import com.srotya.sidewinder.core.storage.gorilla.archival.TimeSeriesArchivalObject;

/**
 * @author ambud
 */
public class S3Archiver implements Archiver {

	private AmazonS3 s3;
	private String bucketName;
	private String prefix;

	@Override
	public void init(Map<String, String> conf) throws IOException {
		AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
		switch (conf.getOrDefault("s3.credential.type", "instance").toLowerCase()) {
		case "instance":
			builder.setCredentials(new InstanceProfileCredentialsProvider(true));
			break;
		case "static":
			AWSCredentials credentials = new BasicAWSCredentials(conf.get("s3.access.key"),
					conf.get("s3.access.secret"));
			builder.setCredentials(new AWSStaticCredentialsProvider(credentials));
			break;
		default:
			throw new IOException("Unkown credential provider");
		}
		builder.setRegion(conf.getOrDefault("s3.region", Regions.US_EAST_1.getName()));
		s3 = builder.build();
		bucketName = conf.getOrDefault("s3.bucket", "archiver");
		prefix = conf.getOrDefault("s3.prefix", "sidewinder");
	}

	@Override
	public void archive(TimeSeriesArchivalObject object) throws ArchiveException {
		try {
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			DataOutputStream os = new DataOutputStream(bytes);
			DiskArchiver.serializeToStream(os, new TimeSeriesArchivalObject(object.getDb(), object.getMeasurement(),
					object.getKey(), object.getBucket()));
			os.close();
			bytes.close();
			ByteArrayInputStream input = new ByteArrayInputStream(bytes.toByteArray());
			ObjectMetadata metadata = new ObjectMetadata();
			PutObjectRequest req = new PutObjectRequest(bucketName, prefix + "/" + System.currentTimeMillis(), input,
					metadata);
			s3.putObject(req);
		} catch (IOException e) {
			throw new ArchiveException(e);
		}
	}

	@Override
	public List<TimeSeriesArchivalObject> unarchive() throws ArchiveException {
		List<TimeSeriesArchivalObject> list = new ArrayList<>();
		ObjectListing objects = s3.listObjects(bucketName, prefix);
		for (S3ObjectSummary summary : objects.getObjectSummaries()) {
			S3Object object = s3.getObject(bucketName, summary.getKey());
			DataInputStream dis = new DataInputStream(object.getObjectContent());
			try {
				list.add(DiskArchiver.deserializeFromStream(dis));
			} catch (IOException e) {
				throw new ArchiveException(e);
			}
		}
		return list;
	}
}
