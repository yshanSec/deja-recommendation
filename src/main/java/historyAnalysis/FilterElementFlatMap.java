package historyAnalysis;

import common.ErrorStatus;
import common.FieldProcessor;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.json.JSONObject;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * Created by yishan on 1/8/16.
 */
public class FilterElementFlatMap implements PairFlatMapFunction<String, String, Integer> {
    public Iterable<Tuple2<String, Integer>> call(String line){
        ArrayList<Tuple2<String, Integer>> results = new ArrayList<Tuple2<String, Integer>>();
//        process line
        String key;
        String[] filterInfo = line.split("\t");
        key = filterInfo[0];
        JSONObject json = new JSONObject(filterInfo[2]);
        ArrayList<String> keyList = new ArrayList<String>();
        keyList.addAll(json.keySet());
        String operation = (String) keyList.toArray()[0];

//        process filter
        JSONObject filter = json.getJSONObject(operation);
        String category = null;
        try{
            category= filter.getString("category");
            category = FieldProcessor.process("category",category);
            results.add(new Tuple2<String, Integer>(key.concat("\tcategory\t").concat(category), 1));
        }
        catch(Exception e){

        }
        String subcategory = null;
        try{
            subcategory = filter.getString("sub_category");
            subcategory = FieldProcessor.process("subcategory",subcategory);
            results.add(new Tuple2<String, Integer>(key.concat("\tsubcategory\t").concat(subcategory), 1));
        }
        catch (Exception e){
            subcategory = ErrorStatus.NOT_FOUND;
        }

        String brand = null;
        try{
            brand = filter.getString("brand");
            brand = FieldProcessor.process("brand",brand);
            results.add(new Tuple2<String, Integer>(key.concat("\tbrand\t").concat(brand), 1));
        }
        catch (Exception e){
            brand = ErrorStatus.NOT_FOUND;
        }

        ArrayList<String> priceRange = new ArrayList<String>();
        String priceMin="";
        try{
            priceMin = filter.getString("price_min");
        }
        catch (Exception e){
            priceMin ="0";
        }
        finally {
            priceRange.add(priceMin);
        }

        String priceMax="";
        try{
            priceMax = filter.getString("price_max");
        }
        catch (Exception e){
            priceMax ="100000000";
        }
        finally {
            priceRange.add(priceMax);
        }
        ArrayList<String> priceScope = FieldProcessor.processPriceRange(priceRange);
        for(int i = 0; i < priceScope.size(); i++){
            if(priceScope.get(i) != ErrorStatus.KEY_FOBIDDEN && priceScope.get(i) != ErrorStatus.NOT_FOUND
                    && subcategory != ErrorStatus.KEY_FOBIDDEN && subcategory != ErrorStatus.NOT_FOUND)
            results.add(new Tuple2<String, Integer>(key.concat("\tprice\t").concat(priceScope.get(i)), 1));
        }
        return results;
    }
}
