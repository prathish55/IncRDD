/***************************************************************************
 * File: CuckooHashMap.java
 *
 * An implementation of a hash map backed by a cuckoo hash table.  Cuckoo hash
 * tables, first described in "Cuckoo Hashing" by Pugh and Rodler, is a hash
 * system with worst-case constant-time lookup and deletion, and amortized 
 * expected O(1) insertion.
 *
 * Internally, cuckoo hashing works by maintaining two arrays of some size,
 * along with two universal hash functions f and g.  When an element x is
 * inserted, the value f(x) is computed and the entry is stored in that spot
 * in the first array.  If that spot was initially empty, we are done.
 * Otherwise, the element that was already there (call it y) is "kicked out."
 * We then compute g(y) and store element y at position g(y) in the second
 * array, which may in turn kick out another element, which will be stored in
 * the first array.  This process repeats until either a loop is detected (in
 * which case we pick a new hash function and rehash), or all elements finally
 * come to rest.
 *
 * The original paper by Pugh and Rodler proves a strong bound - for any 
 * epsilon greater than zero, if the load factor of a Cuckoo hash table is at
 * most (1/2 - epsilon)n, then both the expected runtime and variance of
 * the expected runtime for an insertion is amortized O(1).  This means that
 * we will always want to keep the load factor at just below 50%, say, at 40%.
 *
 * The main challenge of implementing a cuckoo hash table in Java is that the
 * hash code provided for each object is not drawn from a universal hash
 * function.  To ameliorate this, internally we will choose a universal hash
 * function to apply to the hash code of each element.  This is only a good
 * hash if the hash codes for objects are distributed somewhat uniformly, but
 * we assume that this is the case.  If it isn't true - and in particular, if
 * more than two objects of the type hash to the same value - then all bets
 * are off and the cuckoo hash table will entirely fail.  Internally, the
 * class provides a default hash function (described below), but complex class
 * implementations should provide their own implementation.
 *
 * Our family of universal hash functions is based on the universal hash
 * functions described by Mikkel Thorup in "String Hashing for Linear Probing."
 * We begin by breaking the input number into two values, a lower 16-bit value
 * and an upper 16-bit value (denoted HIGH and LOW), then picking two random
 * 32-bit values A and B (which will remain constant across any one hash 
 * function from this family).  We then compute 
 *
 *           HashCode = ((HIGH + A) * (LOW * B)) / (2^(32 - k))
 *
 * Where 2^k is the number of buckets we're hashing into.
 */
package com.prat;
import java.util.*;
import java.util.Map.Entry;
import scala.Tuple2;
import java.io.Serializable;

@SuppressWarnings("unchecked")
public final class CuckooHashMap<K, V> extends AbstractMap<K, V> implements Serializable{
	// load factor < 50%
	private static final float kMaxLoadFactor = 0.40f;

	// The two hash arrays.
	private Entry<K, V> cuckooArrays[][] = new Entry[2][4];

	// The two hash functions.
	private final HashFunction<? super K> cuckooHashFns[] = new HashFunction[2];
	private final UniversalHashFunction<? super K> universalHashFunction;
	private int hashSize = 0;

	private static final class DefaultHashFunction<T> implements
			HashFunction<T> {
		private final int A, B;
		private final int LgSize;

		public DefaultHashFunction(int a, int b, int lgSize) {
			A = a;
			B = b;
			LgSize = lgSize;
		}

		// Given an object, evaluates its hash code.
		public int hash(T obj) {
			if (obj == null)
				return 0;

			// split its hash code into upper and lower bits.
			final int objHash = obj.hashCode();
			final int upper = objHash >>> 16;
			final int lower = objHash & (0xFFFF);

			// compute the hash code using formula
			// HashCode = ((HIGH + A) * (LOW * B)) / (2^(32 - k))
			return (upper * A + lower * B) >>> (32 - LgSize);
		}
	};

	private static final class DefaultUniversalHashFunction<T> implements
			UniversalHashFunction<T> {
		private final Random mRandom = new Random();

		// Produces a HashFunction from the given bucket size.
		public HashFunction<T> randomHashFunction(int numBuckets) {
			int lgBuckets = -1;
			for (; numBuckets > 0; numBuckets >>>= 1)
				++lgBuckets;

			// Return a default hash function initialized with random values
			// and the log of the number of buckets.
			//
			return new DefaultHashFunction<T>(mRandom.nextInt(),
					mRandom.nextInt(), lgBuckets);
		}
	}

	@SuppressWarnings("rawtypes")
	public CuckooHashMap() {
		this(new DefaultUniversalHashFunction());
	}

	public CuckooHashMap(UniversalHashFunction<? super K> func) {

		if (func == null)
			throw new NullPointerException(
					"Universal hash function must be non-null.");

		// Store the family
		universalHashFunction = func;
		// Set up the hash functions.
		generateHashFunc();
	}

	@Override
	public V put(K key, V value) {

		for (int i = 0; i < 2; ++i) {
			// Compute the hash code, then look up the entry there.
			final int hash = cuckooHashFns[i].hash(key);
			final Entry<K, V> entry = cuckooArrays[i][hash];

			/* If the entry matches, we found what we're looking for. */
			if (entry != null && isEqual(entry.getKey(), key)) {
				V result = entry.getValue();
				entry.setValue(value);
				return result;
			}
		}

		// The value is not in the hash table, so we're going to have to insert.
		// check for size
		if (size() >= kMaxLoadFactor * cuckooArrays[0].length * 2)
			grow();

		// Otherwise, continuously try to insert the value into the hash table,
		// rehashing whenever that fails.

		Entry<K, V> toInsert = new SimpleEntry<K, V>(key, value);
		while (true) {
			// Add the entry to the table, then see what element was displaced.
			toInsert = loopInsert(toInsert);
			if (toInsert == null)
				break;
			// Otherwise, rehash and try again.
			rehash();
		}
		++hashSize;
		return null;
	}

	private Entry<K, V> loopInsert(Entry<K, V> toInsert) {

		for (int i = 0; i < size() + 1; ++i) {
			// Compute the hash code and see what's at that position.
			final int hashCode = cuckooHashFns[i % 2].hash(toInsert.getKey());
			final Entry<K, V> entry = cuckooArrays[i % 2][hashCode];

			if (entry == null) {
				cuckooArrays[i % 2][hashCode] = toInsert;
				return null;
			}
			cuckooArrays[i % 2][hashCode] = toInsert;
			toInsert = entry;
		}

		return toInsert;
	}

	private static <T> boolean isEqual(T x, T y) {
		if (x == null && y == null)
			return true;
		if (x == null || y == null)
			return false;
		return x.equals(y);
	}

	private void generateHashFunc() {
		for (int i = 0; i < 2; ++i)
			cuckooHashFns[i] = universalHashFunction
					.randomHashFunction(cuckooArrays[0].length);
	}

	private void rehash() {
		// Begin by creating an array of elements suitable for holding all the
		// elements in the hash table.
		Entry<K, V> values[] = entrySet().toArray(new Entry[0]);

		reinsert: while (true) {
			// Clear all the arrays.
			for (int i = 0; i < 2; ++i)
				Arrays.fill(cuckooArrays[i], null);

			/* Pick two new hash functions. */
			generateHashFunc();

			// Try adding everything.
			for (Entry<K, V> entry : values) {
				if (loopInsert(entry) != null)
					continue reinsert;
			}
			break;
		}
	}

	private void grow() {
		Entry<K, V> oldArrays[][] = cuckooArrays;

		// Reallocate the arrays twice as large as they are now.
		cuckooArrays = new Entry[2][cuckooArrays[0].length * 2];
		int writePoint = 0;
		for (int i = 0; i < 2; ++i)
			for (Entry<K, V> entry : oldArrays[i])
				if (entry != null) // Only write valid entries.
					cuckooArrays[0][writePoint++] = entry;

		// Rehash the array to put everything back in the right place.
		rehash();
	}

	@Override
	public int size() {
		return hashSize;
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public void clear() {
		cuckooArrays = new Entry[2][4];
		hashSize = 0;
		generateHashFunc();
	}

	@Override
	public boolean containsKey(Object key) {
		// Check both locations where the object could be.
		for (int i = 0; i < 2; ++i) {
			final int hash = cuckooHashFns[i].hash((K) key);
			if (cuckooArrays[i][hash] != null
					&& isEqual(cuckooArrays[i][hash].getKey(), key))
				return true;
		}
		return false;
	}

	@Override
	public V get(Object key) {
		// Check both locations where the object could be.
		for (int i = 0; i < 2; ++i) {
			final int hash = cuckooHashFns[i].hash((K) key);
			if (cuckooArrays[i][hash] != null
					&& isEqual(cuckooArrays[i][hash].getKey(), key))
				return cuckooArrays[i][hash].getValue();
		}
		return null;
	}

	@Override
	public V remove(Object key) {
		for (int i = 0; i < 2; ++i) {
			final int hash = cuckooHashFns[i].hash((K) key);
			if (cuckooArrays[i][hash] != null
					&& isEqual(cuckooArrays[i][hash].getKey(), key)) {
				// Cache the value to return.
				V result = cuckooArrays[i][hash].getValue();
				cuckooArrays[i][hash] = null;
				--hashSize;
				return result;
			}
		}
		return null;
	}

	public final class EntrySet extends AbstractSet<Entry<K, V>> {
		@Override
		public int size() {
			return CuckooHashMap.this.size();
		}

		@Override
		public boolean contains(Object entry) {
			if (entry == null)
				return false;

			// Cast it to an Entry<?, ?> and see if the key is contained.
			Entry<?, ?> realEntry = (Entry) entry;
			if (!CuckooHashMap.this.containsKey(realEntry.getKey()))
				return false;

			// Get the value and check if it matches.
			V value = CuckooHashMap.this.get(realEntry.getKey());
			return CuckooHashMap.isEqual(value, realEntry.getValue());
		}

		@Override
		public boolean remove(Object entry) {
			if (!contains(entry))
				return false;
			Entry<?, ?> realEntry = (Entry) entry;
			CuckooHashMap.this.remove(realEntry.getKey());
			return true;
		}

		@Override
		public void clear() {
			CuckooHashMap.this.clear();
		}

		public final class MapIterator implements Iterator<Entry<K, V>> {
			private int nextArr = 0, nextIndex = 0;
			private Entry<K, V> last = null;

			public MapIterator() {
				stageNext();
			}

			public boolean hasNext() {
				return nextArr != 2;
			}

			public Entry<K, V> next() {
				if (!hasNext())
					throw new NoSuchElementException("Out of elements.");
				Entry<K, V> result = cuckooArrays[nextArr][nextIndex];
				++nextIndex;
				stageNext();
				last = result;
				return result;
			}

			public void remove() {
				if (last == null)
					throw new IllegalStateException("No element staged.");
				EntrySet.this.remove(last);
				last = null;
			}

			private void stageNext() {
				for (; nextArr < 2; ++nextArr) {
					for (; nextIndex < cuckooArrays[0].length; ++nextIndex)
						if (cuckooArrays[nextArr][nextIndex] != null)
							return;
					nextIndex = 0;
				}
			}
		}

		public Iterator<Entry<K, V>> iterator() {
			return new MapIterator();
		}
	}

	@Override
	public EntrySet entrySet() {
		return new EntrySet();
	}

	public final class MyMapIterator implements Iterator<Tuple2<K, V>> {
			private int nextArr = 0, nextIndex = 0;
			private Entry<K, V> last = null;

			public MyMapIterator() {
				stageNext();
			}

			public boolean hasNext() {
				return nextArr != 2;
			}

			public Tuple2<K, V> next() {
				if (!hasNext())
					throw new NoSuchElementException("Out of elements.");
				Entry<K, V> result = cuckooArrays[nextArr][nextIndex];
				++nextIndex;
				stageNext();
				last = result;
				return new Tuple2<K,V>(result.getKey(),result.getValue());
			}

			public void remove() {
				if (last == null)
					throw new IllegalStateException("No element staged.");
				//EntrySet.this.remove(last);
				last = null;
			}

			private void stageNext() {
				for (; nextArr < 2; ++nextArr) {
					for (; nextIndex < cuckooArrays[0].length; ++nextIndex)
						if (cuckooArrays[nextArr][nextIndex] != null)
							return;
					nextIndex = 0;
				}
			}
		}

	public Iterator<Tuple2<K, V>> iterator() {
		return new MyMapIterator();
	}
}
