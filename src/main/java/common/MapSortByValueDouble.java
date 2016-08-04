package common;

/**
 * Created by yishan on 3/8/16.
 */
import java.util.Comparator;
import java.util.Map;

public class MapSortByValueDouble implements Comparator<Map.Entry<String, Double>> {
    public int compare(Map.Entry<String, Double> entry1, Map.Entry<String, Double> entry2){
        return (entry1.getValue()).compareTo(entry2.getValue());
    }

}
class MapSortByValueInteger implements Comparator<Map.Entry<String, Integer>> {
    public int compare(Map.Entry<String, Integer> entry1, Map.Entry<String, Integer> entry2){
        return (entry1.getValue()).compareTo(entry2.getValue());
    }

}
