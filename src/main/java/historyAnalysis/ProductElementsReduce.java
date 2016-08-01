package historyAnalysis;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.ReduceFunction;

/**
 * Created by yishan on 1/8/16.
 */
public class ProductElementsReduce implements Function2<Integer, Integer, Integer> {
    public Integer call(Integer num1, Integer num2){
        return num1+num2;
    }
}
