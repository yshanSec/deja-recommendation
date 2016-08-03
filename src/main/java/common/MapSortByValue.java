package common;

/**
 * Created by yishan on 3/8/16.
 */
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class MapSortByValue implements Comparator {
    Map map;

    public MapSortByValue(Map map) {
        this.map = map;
    }

    public int compare(Object keyA, Object keyB) {
        Comparable valueA = (Comparable) map.get(keyA);
        Comparable valueB = (Comparable) map.get(keyB);
        return valueB.compareTo(valueA);
    }
}
