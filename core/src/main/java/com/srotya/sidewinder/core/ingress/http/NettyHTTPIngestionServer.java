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
package com.srotya.sidewinder.core.ingress.http;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.gorilla.MemStorageEngine;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

/**
 * 
 * 
 * @author ambud
 */
public class NettyHTTPIngestionServer {

	private Channel channel;
	private StorageEngine storageEngine;

	public void init(StorageEngine storageEngine, Map<String, String> conf) {
		this.storageEngine = storageEngine;
	}

	public void start() throws InterruptedException {
		EventLoopGroup bossGroup = new NioEventLoopGroup(1);
		EventLoopGroup workerGroup = new NioEventLoopGroup(4);

		ServerBootstrap bs = new ServerBootstrap();
		channel = bs.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
				.option(ChannelOption.SO_RCVBUF, 10485760).option(ChannelOption.SO_SNDBUF, 10485760)
				.handler(new LoggingHandler(LogLevel.DEBUG)).childHandler(new ChannelInitializer<SocketChannel>() {

					@Override
					protected void initChannel(SocketChannel ch) throws Exception {
						ChannelPipeline p = ch.pipeline();
						p.addLast(new HttpRequestDecoder());
						p.addLast(new HttpResponseEncoder());
						p.addLast(new HTTPDataPointDecoder(storageEngine));
					}

				}).bind("localhost", 9928).sync().channel();
	}

	public void stop() throws InterruptedException {
		channel.closeFuture().await();
	}

	public static void main(String[] args) throws InterruptedException, IOException {
		StorageEngine engine = new MemStorageEngine();
		engine.configure(new HashMap<>());
		NettyHTTPIngestionServer server = new NettyHTTPIngestionServer();
		server.init(engine, new HashMap<>());
		server.start();
	}

}
