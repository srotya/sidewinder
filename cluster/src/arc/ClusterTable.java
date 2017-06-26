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
package com.srotya.sidewinder.cluster.atomix;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.Logger;

import io.atomix.AtomixReplica;
import io.atomix.catalyst.transport.Address;
import io.atomix.collections.DistributedSet;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.group.DistributedGroup;
import io.atomix.group.GroupMember;
import io.atomix.group.LocalMember;
import io.atomix.group.election.Term;
import io.atomix.group.messaging.Message;
import io.atomix.group.messaging.MessageProducer;

/**
 * @author ambud
 */
public class ClusterTable {

	private static Logger logger = Logger.getLogger(ClusterTable.class.getName());
	private static final String DB_SET = "dbs";
	private static final String BROADCAST = "broadcast";
	private Map<String, DistributedGroup> groupMap;
	private AtomixReplica replica;
	private DistributedGroup messagingGroup;
	private MessageProducer<Object> producer;
	private LocalMember localMember;
	private Node localNode;

	public ClusterTable(AtomixReplica replica, Node localNode) {
		this.replica = replica;
		this.localNode = localNode;
		this.groupMap = new ConcurrentHashMap<>();
	}

	public void init() throws InterruptedException, ExecutionException {
		messagingGroup = replica.getGroup(BROADCAST).join();
		localMember = messagingGroup.join(localNode).join();
		localMember.messaging().consumer(BROADCAST).onMessage(new Consumer<Message<Object>>() {

			@Override
			public void accept(Message<Object> t) {
				try {
					getLeader(t.message().toString());
					t.ack();
				} catch (InterruptedException | ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		producer = messagingGroup.messaging().producer(BROADCAST);
		if (replica.exists(DB_SET).get()) {
			DistributedSet<Object> set = replica.getSet(DB_SET).get();
			Iterator<Object> itr = set.iterator().get();
			while (itr.hasNext()) {
				String dbname = itr.next().toString();
				groupMap.put(dbname, replica.getGroup(dbname).join());
			}
			logger.info("Databases loaded:" + groupMap.size());
		} else {
			logger.info("No databases found");
			// create set
		}
	}

	public GroupMember getLeader(String dbName) throws InterruptedException, ExecutionException {
		DistributedGroup group = groupMap.get(dbName);
		if (group == null) {
			group = replica.getGroup(dbName).join();
			group.join(localNode).get();
			group.onJoin(new Consumer<GroupMember>() {

				@Override
				public void accept(GroupMember t) {
					// logger.info("New member:" + t);
				}
			});
			groupMap.put(dbName, group);
			producer.send(dbName).get();
			// logger.info("Electing a new leader for database:" + dbName);
			final AtomicBoolean election = new AtomicBoolean(false);
			group.election().onElection(new Consumer<Term>() {

				@Override
				public void accept(Term t) {
					// logger.info("Leader election completed:" + t);
					election.set(true);
				}
			});
			// logger.info("Waiting for leader election.");
			while (!election.get()) {
				System.out.print(".");
				Thread.sleep(100);
			}
		}
		return group.election().term().leader();
	}

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		Storage storage = Storage.builder().withCompactionThreads(2).withStorageLevel(StorageLevel.MEMORY).build();
		AtomixReplica.Builder builder = AtomixReplica.builder(new Address("localhost", 8900))
				.withElectionTimeout(Duration.ofSeconds(3)).withHeartbeatInterval(Duration.ofSeconds(2))
				.withStorage(storage);
		AtomixReplica replica = builder.build().bootstrap(new Address("localhost", 8900)).join();
		ClusterTable table = new ClusterTable(replica, new Node("localhost", 8991));
		table.init();
		long ts = System.currentTimeMillis();
		for (int i = 0; i < 40_000_000; i++) {
			table.getLeader("dbx").metadata();
			if (i % 1000 == 0) {
				System.out.println(i + " " + (System.currentTimeMillis() - ts));
			}
		}
		System.out.println("Completed election for 10k partitions in:" + (System.currentTimeMillis() - ts));
	}

}
