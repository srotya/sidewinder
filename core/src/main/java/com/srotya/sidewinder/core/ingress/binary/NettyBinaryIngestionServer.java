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
package com.srotya.sidewinder.core.ingress.binary;

import java.util.Map;

import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.utils.BackgrounThreadFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

/**
 * A Netty based binary ingress server.
 * 
 * @author ambud
 */
public class NettyBinaryIngestionServer {

	private Channel channel;
	private StorageEngine storageEngine;

	public void init(StorageEngine storageEngine, Map<String, String> conf) {
		this.storageEngine = storageEngine;
	}

	public void start() throws InterruptedException {
		EventLoopGroup bossGroup = new NioEventLoopGroup(1, new BackgrounThreadFactory("binBossGroup"));
		EventLoopGroup workerGroup = new NioEventLoopGroup(1, new BackgrounThreadFactory("binWorkerGroup"));
		EventLoopGroup decoderGroup = new NioEventLoopGroup(1, new BackgrounThreadFactory("binDecoderGroup"));
		EventLoopGroup writerGroup = new NioEventLoopGroup(2, new BackgrounThreadFactory("binWriterGroup"));

		ServerBootstrap bs = new ServerBootstrap();
		channel = bs.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
				.option(ChannelOption.SO_RCVBUF, 10485760).option(ChannelOption.SO_SNDBUF, 10485760)
				.handler(new LoggingHandler(LogLevel.DEBUG)).childHandler(new ChannelInitializer<SocketChannel>() {

					@Override
					protected void initChannel(SocketChannel ch) throws Exception {
						ChannelPipeline p = ch.pipeline();
						p.addLast(new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4));
						p.addLast(decoderGroup, new SeriesDataPointDecoder());
						p.addLast(writerGroup, new SeriesDataPointWriter(storageEngine));
					}

				}).bind("localhost", 9927).sync().channel();
	}

	public void stop() throws InterruptedException {
		channel.closeFuture().await();
	}
}
