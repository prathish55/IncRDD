/***************************************************************************
 * File: UniversalHashFunction.java
 * 
 * An object representing a family of universal hash functions. The object can
 * then hand back random instances of the universal hash function on an
 * as-needed basis.
 */
package com.prat;

public interface UniversalHashFunction<T> {
	/**
	 * Given as input the number of buckets, produces a random hash function
	 * that hashes objects of type T into those buckets with the guarantee that
	 * 
	 * <pre>
	 *             Pr[h(x) == h(y)] <= |T| / numBuckets, for all x != y
	 * </pre>
	 */
	public HashFunction<T> randomHashFunction(int buckets);
}
