package com.srotya.sidewinder.core.predicates;

import java.util.ArrayList;
import java.util.List;

public abstract class ComplexPredicate implements Predicate {

	private List<Predicate> predicates;

	public ComplexPredicate(List<Predicate> predicates) {
		if(predicates==null) {
			this.predicates = new ArrayList<>();
		}else {
			this.predicates = predicates;
		}
	}
	
	@Override
	public boolean apply(long value) {
		boolean result = predicates.get(0).apply(value);
		for (int i = 1; i < predicates.size(); i++) {
			boolean temp = result;
			result = predicate(result, predicates.get(i), value);
			if (shortCircuit(temp, result)) {
				break;
			}
		}
		return result;
	}
	
	public abstract boolean shortCircuit(boolean prev, boolean current);

	public abstract boolean predicate(boolean prev, Predicate next, long value);

	public void addPredicate(Predicate predicate) {
		predicates.add(predicate);
	}
	
	public void addPredicates(List<Predicate> operators) {
		predicates.addAll(operators);
	}

	public List<Predicate> getPredicates() {
		return predicates;
	}

}
