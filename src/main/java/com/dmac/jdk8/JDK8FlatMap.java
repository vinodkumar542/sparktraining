package com.dmac.jdk8;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public class JDK8FlatMap {

	public static void main(String[] args) {
		List<List<Integer>> listOfInteger = new ArrayList<>();
		
		List<Integer> a = new ArrayList<>();
		a.add(1);a.add(2);a.add(3);

		List<Integer> b = new ArrayList<>();
		b.add(4);b.add(5);b.add(6);
		
		List<Integer> c = new ArrayList<>();
		c.add(7);c.add(8);c.add(9);
		
		listOfInteger.add(a);
		listOfInteger.add(b);
		listOfInteger.add(c);
		
		Stream<Integer> integerStream = listOfInteger.stream().flatMap(Collection::stream);
		integerStream.forEach(param -> System.out.println(param));
	}

}
