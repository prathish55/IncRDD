/****************************************************************************
 * File: HashFunction.java
 * 
 * An object representing a hash function capable of hashing objects of some
 * type. This allows the notion of a hash function to be kept separate from the
 * object itself and is necessary to provide families of hash functions.
 */
package com.prat;

public interface HashFunction<T> {
	// Given an object, returns the hash code of that object.
	public int hash(T obj);
}
