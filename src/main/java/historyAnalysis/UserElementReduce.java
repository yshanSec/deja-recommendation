package historyAnalysis;

import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by yishan on 1/8/16.
 */
public class UserElementReduce
        implements Function2<HashMap<String, HashMap<String, Integer>>,
                            HashMap<String, HashMap<String, Integer>>,
                            HashMap<String, HashMap<String, Integer>>> {
    public HashMap<String, HashMap<String, Integer>> call(HashMap<String, HashMap<String, Integer>> value1,
                                                                          HashMap<String, HashMap<String, Integer>> value2){

        HashMap<String, HashMap<String, Integer>> value = new HashMap<String, HashMap<String, Integer>>();
        ArrayList<String> fieldArray = new ArrayList<String>();
        fieldArray.addAll(value1.keySet());
        fieldArray.addAll(value2.keySet());
        Set<String> keySet = new HashSet<String>(fieldArray);
        fieldArray.addAll(keySet);

        for(int i = 0; i <fieldArray.size(); i++){
            HashMap<String, Integer> map = new HashMap<String, Integer>();

            HashMap<String, Integer> map1 = value1.get(fieldArray.get(i));
            ArrayList<String> keyArray1 = new ArrayList<String>();
            if(map1 != null){
                keyArray1.addAll(map1.keySet());
            }
            for(int j =0; j < keyArray1.size(); j++){
                map.put(keyArray1.get(j), map1.get(keyArray1.get(j)));
            }

            HashMap<String, Integer> map2 = value2.get(fieldArray.get(i));
            ArrayList<String> keyArray2 = new ArrayList<String>();
            if(map2 != null){
                keyArray2.addAll(map2.keySet());
            }
            for(int j =0; j < keyArray2.size(); j++){
                map.put(keyArray2.get(j), map2.get(keyArray2.get(j)));
            }
            value.put(fieldArray.get(i), map);
        }
        return value;
    }
}
