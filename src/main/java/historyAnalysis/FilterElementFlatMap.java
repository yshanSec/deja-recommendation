package historyAnalysis;

import common.ErrorStatus;
import common.FieldProcessor;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.json.JSONObject;
import org.json.JSONException;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * Created by yishan on 1/8/16.
 */
public class FilterElementFlatMap implements PairFlatMapFunction<String, String, Integer> {
    public Iterable<Tuple2<String, Integer>> call(String line) throws JSONException{
        ArrayList<Tuple2<String, Integer>> results = new ArrayList<Tuple2<String, Integer>>();
//        process line
        String key;
        String[] filterInfo = line.split("\t");
        key = filterInfo[0];
        JSONObject json = new JSONObject(filterInfo[2]);
        ArrayList<String> keyList = new ArrayList<String>(IteratorUtils.toList(json.keys()));

        String operation = (String) keyList.toArray()[0];

//        process filter
        JSONObject filter = json.getJSONObject(operation);
        String category = null;
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
            if((! priceScope.get(i).equals(ErrorStatus.KEY_FOBIDDEN))  && (!priceScope.get(i).equals(ErrorStatus.NOT_FOUND))
                    && (! subcategory.equals(ErrorStatus.KEY_FOBIDDEN)) && (! subcategory.equals(ErrorStatus.NOT_FOUND)))
            results.add(new Tuple2<String, Integer>(key.concat("\tprice\t").concat(subcategory).concat("-").concat(priceScope.get(i)), 1));
        }
        return results;
    }
}
