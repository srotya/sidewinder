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
package com.srotya.sidewinder.cluster.connectors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;

public class CuratorLeaderElection {

	public void init() {

	}

	@SuppressWarnings("resource")
	public static void main(String[] args) throws Exception {
		CuratorFramework curator = CuratorFrameworkFactory.newClient("localhost:2181",
				new BoundedExponentialBackoffRetry(1000, 5000, 60));
		curator.start();
		curator.getZookeeperClient().blockUntilConnectedOrTimedOut();
		System.out.println("Connected");
		LeaderLatch leader1 = new LeaderLatch(curator, "/sidewinder", "0");
		leader1.addListener(new LeaderLatchListener() {

			@Override
			public void notLeader() {
				System.out.println("Not leader");
				try {
					leader1.getLeader();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			@Override
			public void isLeader() {
				System.out.println("Became leader");
			}
		});
		leader1.start();
		
		LeaderLatch leader2 = new LeaderLatch(curator, "/sidewinder", "1");
		leader2.addListener(new LeaderLatchListener() {

			@Override
			public void notLeader() {
				try {
					leader2.getLeader();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			@Override
			public void isLeader() {
				System.out.println("Became leader");
			}
		});
		leader2.start();

		Thread.sleep(100000);
	}

}
