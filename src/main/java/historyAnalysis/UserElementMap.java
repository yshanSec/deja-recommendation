package historyAnalysis;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by yishan on 1/8/16.
 */
public class UserElementMap implements PairFunction<Tuple2<String, Integer>, String, HashMap<String, HashMap<String, Integer>>> {
    public Tuple2<String, HashMap<String, HashMap<String, Integer>>> call(Tuple2<String, Integer> element){
        String [] keyInfo = element._1.split("\t");
        String uid = keyInfo[0];
        String field = keyInfo[1];
        String fieldValue = keyInfo[2];
        Integer num = element._2;

        String key = uid;
        HashMap<String, HashMap<String, Integer>> value = new HashMap<String, HashMap<String, Integer>>();
        HashMap<String, Integer> valueCount = new HashMap<String, Integer>();
        valueCount.put(fieldValue,num);
        value.put(field,valueCount);
        return new Tuple2<String, HashMap<String, HashMap<String, Integer>>>(key, value);
    }
}
