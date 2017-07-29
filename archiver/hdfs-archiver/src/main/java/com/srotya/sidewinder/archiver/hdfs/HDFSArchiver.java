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
package com.srotya.sidewinder.archiver.hdfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;

import com.srotya.sidewinder.core.storage.ArchiveException;
import com.srotya.sidewinder.core.storage.Archiver;
import com.srotya.sidewinder.core.storage.mem.archival.DiskArchiver;
import com.srotya.sidewinder.core.storage.mem.archival.TimeSeriesArchivalObject;


/**
 * @author ambud
 */
public class HDFSArchiver implements Archiver {

	private static final String HDFS_KERBEROS_KEYTAB = "hdfs.kerberos.keytab";
	private static final String HDFS_KERBEROS_USER = "hdfs.kerberos.user";
	private static final String HDFS_KERBEROS = "hdfs.kerberos";
	private static final String MAX_FILE_SIZE = "max.file.size";
	public static final String HDFS_ARCHIVE_DIRECTORY = "hdfs.archive.directory";
	private static final String HDFS_HDFS_SITE = "hdfs.hdfs.site";
	private static final String HDFS_CORE_SITE = "hdfs.core.site";
	private Configuration configuration;
	private FileSystem fs;
	private Path archiveDirectory;
	private FsPermission fsPermission;
	private DataOutputStream os;
	private long maxFileSize;
	private Path currentFile;

	@Override
	public void init(Map<String, String> conf) throws IOException {
		configuration = new Configuration();
		maxFileSize = Long.parseLong(conf.getOrDefault(MAX_FILE_SIZE, String.valueOf(1024 * 1024 * 10)));
		if (conf.containsKey(HDFS_CORE_SITE)) {
			configuration.addResource(new FileInputStream(conf.get(HDFS_CORE_SITE)));
		}
		if (conf.containsKey(HDFS_HDFS_SITE)) {
			configuration.addResource(new FileInputStream(conf.get(HDFS_HDFS_SITE)));
		}

		if (Boolean.parseBoolean(conf.getOrDefault(HDFS_KERBEROS, "false"))) {
			UserGroupInformation.loginUserFromKeytab(conf.getOrDefault(HDFS_KERBEROS_USER, "hdfs"),
					conf.getOrDefault(HDFS_KERBEROS_KEYTAB, "/etc/security/keytabs/hdfs.service.keytab"));
		}

		fs = FileSystem.get(configuration);
		archiveDirectory = new Path(conf.getOrDefault(HDFS_ARCHIVE_DIRECTORY, "/apps/sidewinder/archive"));
		fsPermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
		fs.mkdirs(archiveDirectory, fsPermission);
		currentFile = new Path(archiveDirectory, new Path("archive." + System.currentTimeMillis() + ".bin"));
		os = new DataOutputStream(new BufferedOutputStream(FileSystem.create(fs, currentFile, fsPermission)));
	}

	@Override
	public void archive(TimeSeriesArchivalObject object) throws ArchiveException {
		try {
			if (fs.getFileStatus(currentFile).getLen() > maxFileSize) {
				os.close();
				currentFile = new Path(archiveDirectory, "archive." + System.currentTimeMillis() + ".bin");
				os = new DataOutputStream(new BufferedOutputStream(FileSystem.create(fs, currentFile, fsPermission)));
			}
			DiskArchiver.serializeToStream(os, new TimeSeriesArchivalObject(object.getDb(), object.getMeasurement(),
					object.getKey(), object.getBucket()));
		} catch (IOException e) {
			throw new ArchiveException(e);
		}
	}

	@Override
	public List<TimeSeriesArchivalObject> unarchive() throws ArchiveException {
		List<TimeSeriesArchivalObject> list = new ArrayList<>();
		try {
			RemoteIterator<LocatedFileStatus> itr = fs.listFiles(archiveDirectory, true);
			while (itr.hasNext()) {
				DataInputStream bis = new DataInputStream(new BufferedInputStream(fs.open(itr.next().getPath())));
				while (bis.available() > 0) {
					TimeSeriesArchivalObject object = DiskArchiver.deserializeFromStream(bis);
					list.add(object);
				}
				bis.close();
			}
		} catch (IOException e) {
			throw new ArchiveException(e);
		}
		return list;
	}

}
