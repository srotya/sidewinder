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
package com.srotya.sidewinder.core;

/**
 * @author ambud
 */
public interface ConfigConstants {
	
	public static final String AUTH_BASIC_USERS = "auth.basic.users";
	public static final String FALSE = "false";
	public static final String TRUE = "true";
	public static final String DEFAULT_STORAGE_ENGINE = "com.srotya.sidewinder.core.storage.mem.MemStorageEngine";
	public static final String STORAGE_ENGINE = "storage.engine";
	public static final String AUTH_BASIC_ENABLED = "auth.basic.enabled";
	public static final String GRPC_PORT = "grpc.port";
	public static final String DEFAULT_GRPC_PORT = "9928";
	public static final String DEFAULT_GRPC_EXECUTOR_COUNT = "1";
	public static final String GRPC_EXECUTOR_COUNT = "grpc.executor.count";
	public static final String ENABLE_GRPC = "grpc.enabled";
	public static final String BG_THREAD_COUNT = "bgthread.count";
	public static final String GRAPHITE_ENABLED = "graphite.enabled";

}
