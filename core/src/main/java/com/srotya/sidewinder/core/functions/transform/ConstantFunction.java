package com.srotya.sidewinder.core.functions.transform;

public abstract class ConstantFunction extends TransformFunction {
	
	protected double constant; 
	
	@Override
	public void init(Object[] args) throws Exception {
		constant = (double) args[0];
	}
	
	@Override
	public int getNumberOfArgs() {
		return 1;
	}

}