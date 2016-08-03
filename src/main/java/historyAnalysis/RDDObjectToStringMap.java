package historyAnalysis;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import org.json.JSONObject;
import java.util.HashMap;

/**
 * Created by yishan on 3/8/16.
 */
public class RDDObjectToStringMap implements Function<Tuple2<String, HashMap<String, HashMap<String, Integer>>>, String> {
    public String call(Tuple2<String, HashMap<String, HashMap<String, Integer>>> record){
        String key = record._1();
        String value = (new JSONObject(record._2())).toString();
        return key.concat("\t").concat(value);
    }
}
