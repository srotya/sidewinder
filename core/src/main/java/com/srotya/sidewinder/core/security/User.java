package com.srotya.sidewinder.core.security;

import java.security.Principal;

public class User implements Principal {
	
	private String name;

	public User(String name) {
		this.name = name;
	}

	@Override
	public String getName() {
		return name;
	}

}
