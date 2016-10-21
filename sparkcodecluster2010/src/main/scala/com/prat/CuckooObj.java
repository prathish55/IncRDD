package com.prat;
import java.util.ArrayList;
import java.util.Set;

public class CuckooObj {
	// cmap is reference to main cuckooHashMap
	// not a copy!
	//
	CuckooHashMap<Integer, ArrayList<Integer>> cmap;
	// valMap is a hash map of key and no of values in the array list of that
	// key
	//
	CuckooHashMap<Integer, Integer> valMap;

	public CuckooObj() {
		this.cmap = null;
		valMap = new CuckooHashMap<Integer, Integer>();
	}

	public CuckooObj(CuckooHashMap<Integer, ArrayList<Integer>> cmap) {
		this.cmap = cmap;
		valMap = new CuckooHashMap<Integer, Integer>();
		Set<Integer> e = cmap.keySet();
		for (int i : e)
			valMap.put(i, cmap.get(i).size());
	}

	public void changeValue(int key, int value) {
		// changeValue means that particular object wants to change the value
		// but in implementation, the new value is added to array list
		//
		if (!cmap.containsKey(key)) {
			ArrayList<Integer> alist = new ArrayList<Integer>();
			alist.add(value);
			cmap.put(key, alist);
			valMap.put(key, 1);
		} else {
			ArrayList<Integer> alist = cmap.get(key);
			alist.add(value);
			valMap.put(key, cmap.get(key).size());
		}
	}

	public int getValue(int key) {
		// get the position of last updated value for the key in that object
		//
		int aListLen = valMap.get(key);
		int val = cmap.get(key).get(aListLen - 1);
		System.out.println("key " + key + " is " + val);
		return val;

	}
}
