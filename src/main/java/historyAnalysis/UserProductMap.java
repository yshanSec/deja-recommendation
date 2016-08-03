package historyAnalysis;

import common.InvertedIndex;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.json.JSONObject;
/**
 * Created by yishan on 3/8/16.
 */
public class UserProductMap
        implements PairFunction<Tuple2<String, HashMap<String, HashMap<String, Integer>>>, String, String> {
    public InvertedIndex invertedIndex;
    public UserProductMap(Broadcast<InvertedIndex> invertedIndexBroadcast){
        this.invertedIndex = invertedIndexBroadcast.value();
    }
    public Tuple2<String, String> call(Tuple2<String, HashMap<String, HashMap<String, Integer>>> record){
        String uid = record._1();
        HashMap<String, HashMap<String, Integer>> info = record._2();
        HashMap<String, ArrayList<String>> essentialInfo = invertedIndex.getEssentialElement(info);
        ArrayList<HashMap<String, ArrayList<String>>> essentialFilters = invertedIndex.getEssentailFilters(essentialInfo);

        ArrayList<String> productIds = new ArrayList<String>();
        for(int i = 0; i < essentialFilters.size(); i++){
            productIds.addAll(this.invertedIndex.invertedSearchCombine(essentialFilters.get(i)));
        }
        HashSet<String> productIdsSet = new HashSet<String>(productIds);
        String productIdsString = (new JSONObject(productIdsSet)).toString();
        return new Tuple2<String, String>(uid,productIdsString);
    }
}
