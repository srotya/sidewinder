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
import java.util.function.Consumer;

import io.atomix.AtomixReplica;
import io.atomix.AtomixReplica.Type;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.group.DistributedGroup;
import io.atomix.group.GroupMember;
import io.atomix.group.LocalMember;
import io.atomix.group.election.Term;

public class AtomixSlave {

	public static void main(String[] args) {
		AtomixReplica.Builder builder = AtomixReplica.builder(new Address("localhost", Integer.parseInt(args[0])));
		Storage storage = Storage.builder().withDirectory("target/node" + args[0]).withCompactionThreads(2)
				.withStorageLevel(StorageLevel.MEMORY).build();
		builder.withClientTransport(new NettyTransport()).withElectionTimeout(Duration.ofSeconds(10))
				.withHeartbeatInterval(Duration.ofSeconds(2)).withType(Type.ACTIVE).withStorage(storage);

		AtomixReplica atomix = builder.build();
		atomix = atomix.join(new Address("localhost", 8700)).join();

		DistributedGroup group = atomix.getGroup("side").join();

		group.onJoin(new Consumer<GroupMember>() {

			@Override
			public void accept(GroupMember t) {
				System.out.println("New member:" + t);
			}
		});

		group.onLeave(new Consumer<GroupMember>() {

			@Override
			public void accept(GroupMember t) {
				System.out.println("Leaving member:" + t);
			}
		});

		group.election().onElection(new Consumer<Term>() {

			@Override
			public void accept(Term t) {
				System.out.println("\n\n\nLeader election:" + t + "\n\n\n");
			}
		});
		LocalMember member = group.join().join();
		System.out.println("Local id:" + member.id());
	}

}
