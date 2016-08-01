package historyAnalysis;

import common.ErrorStatus;
import common.FieldProcessor;
import common.ProductSearcher;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.json.JSONArray;
import org.json.JSONObject;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * Created by yishan on 1/8/16.
 */
public class ProductElementsFlatMap implements PairFlatMapFunction<String, String, Integer> {

    public Iterable<Tuple2<String, Integer>> call(String line){
        ArrayList<Tuple2<String, Integer>> results = new ArrayList<Tuple2<String, Integer>>();
//        process line
        String key;
        String[] productVisitInfo = line.split("\t");
        key = productVisitInfo[0];

//        get fields info of product
        JSONObject json = new JSONObject(productVisitInfo[2]);
        JSONArray productIds = json.getJSONArray("product_ids");
        ProductSearcher productSearcher = UserProfiling.productSearcher.value();
        for(int i = 0; i < productIds.length(); i++){
//            fieldList = {"cutting", "color", "price", "brand", "subcategory", "category"};
//            process different fields, cutting contains a list, the other fields only contain a value

            String category = FieldProcessor.process("category", productSearcher.search(productIds.getString(i),"category").get(0));
            if(category != ErrorStatus.KEY_FOBIDDEN && category != ErrorStatus.NOT_FOUND)
                results.add(new Tuple2<String, Integer>(key.concat("\tcategory\t").concat(category), 1));

            String subcategory = FieldProcessor.process("subcategory", productSearcher.search(productIds.getString(i),"subcategory").get(0));
            if(subcategory != ErrorStatus.KEY_FOBIDDEN && subcategory != ErrorStatus.NOT_FOUND)
                results.add(new Tuple2<String, Integer>(key.concat("\tsubcategory\t").concat(subcategory), 1));

            String brand = FieldProcessor.process("brand", productSearcher.search(productIds.getString(i),"brand").get(0));
            if(brand != ErrorStatus.KEY_FOBIDDEN && brand != ErrorStatus.NOT_FOUND)
                results.add(new Tuple2<String, Integer>(key.concat("\tbrand\t").concat(brand), 1));

            String color = FieldProcessor.process("color", productSearcher.search(productIds.getString(i),"color").get(0));
            if(color != ErrorStatus.KEY_FOBIDDEN && color != ErrorStatus.NOT_FOUND)
                results.add(new Tuple2<String, Integer>(key.concat("\tcolor\t").concat(color), 1));

            String price = FieldProcessor.process("price", productSearcher.search(productIds.getString(i),"price").get(0));
            if(price != ErrorStatus.KEY_FOBIDDEN && price != ErrorStatus.NOT_FOUND
                    && subcategory != ErrorStatus.KEY_FOBIDDEN && subcategory != ErrorStatus.NOT_FOUND)
                results.add(new Tuple2<String, Integer>(key.concat("\tprice\t").concat(subcategory).concat("-").concat(price), 1));

            ArrayList<String> cuttingList = productSearcher.search(productIds.getString(i),"cutting");

            for(int j = 0; j < cuttingList.size(); j++){
                if(cuttingList.get(j) != ErrorStatus.KEY_FOBIDDEN && cuttingList.get(j) != ErrorStatus.NOT_FOUND
                        && subcategory != ErrorStatus.KEY_FOBIDDEN && subcategory != ErrorStatus.NOT_FOUND)
                    results.add(new Tuple2<String, Integer>(key.concat("\tcutting\t").concat(subcategory).concat("-").concat(cuttingList.get(j)), 1));
            }

        }
        return results;
    }
}
