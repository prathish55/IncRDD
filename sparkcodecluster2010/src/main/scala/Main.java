import com.prat.*;
import java.util.ArrayList;
import java.util.Set;

public class Main {

	public static CuckooHashMap<Integer, ArrayList<Integer>> cmap;

	public static void main(String[] args) {
		cmap = new CuckooHashMap<Integer, ArrayList<Integer>>();
		CuckooObj obj1 = new CuckooObj(cmap);
		CuckooObj obj2 = new CuckooObj(cmap);

		obj1.changeValue(1, 100);
		obj2.changeValue(1, 101);
		obj1.changeValue(1, 102);
		obj1.changeValue(2, 10);
		obj2.changeValue(5, 3);
		obj2.changeValue(2, 11);

		int val = obj1.getValue(1);

		Set<Integer> e = cmap.keySet();
		for (int i : e)
			System.out.println(i + " " + cmap.get(i));

	}

}
