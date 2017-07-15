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
package com.srotya.sidewinder.core.security;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.basic.BasicCredentials;

/**
 * @author ambud
 */
public class BasicAuthenticator implements Authenticator<BasicCredentials, Principal> {

	private static final Logger logger = Logger.getLogger(BasicAuthenticator.class.getName());
	private static Map<String, String> users = new HashMap<>();

	public BasicAuthenticator(String usersFile) {
		try {
			if(usersFile==null || usersFile.isEmpty()) {
				users.put("admin", "admin");
				return;
			}
			for (String creds : Files.readAllLines(new File(usersFile).toPath())) {
				String[] split = creds.split(":");
				if(split.length!=2) {
					continue;
				}
				users.put(split[0], split[1]);
			}
		} catch (IOException e) {
			logger.log(Level.WARNING, "Unable to load allowed users", e);
		}
	}

	@Override
	public Optional<Principal> authenticate(BasicCredentials credentials) throws AuthenticationException {
		String username = credentials.getUsername();
		String password = credentials.getPassword();
		String pass = users.get(username);
		if(pass!=null && pass.equals(password)) {
			return Optional.of(new User(username)); 
		}
		return Optional.empty();
	}

}
