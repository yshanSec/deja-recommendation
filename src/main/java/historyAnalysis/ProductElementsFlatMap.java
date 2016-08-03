package historyAnalysis;

import common.ErrorStatus;
import common.FieldProcessor;
import common.ProductSearcher;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.json.JSONArray;
import org.json.JSONObject;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * Created by yishan on 1/8/16.
 */
public class ProductElementsFlatMap implements PairFlatMapFunction<String, String, Integer> {
    public ProductSearcher productSearcher;
    public ProductElementsFlatMap(Broadcast<ProductSearcher> productSearcher){
        this.productSearcher = productSearcher.value();
    }
    public Iterable<Tuple2<String, Integer>> call(String line){
        ArrayList<Tuple2<String, Integer>> results = new ArrayList<Tuple2<String, Integer>>();
//        process line
        String key;
        String[] productVisitInfo = line.split("\t");
        key = productVisitInfo[0];
//        ProductSearcher productSearcher = UserProfiling.productSearcher.value();
//        get fields info of product
        JSONObject json = new JSONObject(productVisitInfo[2]);
        JSONArray productIds = json.getJSONArray("product_ids");
        for(int i = 0; i < productIds.length(); i++){
//            fieldList = {"cutting", "color", "price", "brand", "subcategory", "category"};
//            process different fields, cutting contains a list, the other fields only contain a value

            String category = FieldProcessor.process("category", productSearcher.search(productIds.getString(i),"category").get(0));
            if((! category.equals(ErrorStatus.KEY_FOBIDDEN)) && (! category.equals(ErrorStatus.NOT_FOUND)))
                results.add(new Tuple2<String, Integer>(key.concat("\tcategory\t").concat(category), 1));

            String subcategory = FieldProcessor.process("subcategory", productSearcher.search(productIds.getString(i),"subcategory").get(0));
            if((! subcategory.equals(ErrorStatus.KEY_FOBIDDEN)) && (! subcategory.equals(ErrorStatus.NOT_FOUND)))
                results.add(new Tuple2<String, Integer>(key.concat("\tsubcategory\t").concat(subcategory), 1));

            String brand = FieldProcessor.process("brand", productSearcher.search(productIds.getString(i),"brand").get(0));
            if((! brand.equals(ErrorStatus.KEY_FOBIDDEN)) && (! brand.equals(ErrorStatus.NOT_FOUND)))
                results.add(new Tuple2<String, Integer>(key.concat("\tbrand\t").concat(brand), 1));

            String color = FieldProcessor.process("color", productSearcher.search(productIds.getString(i),"color").get(0));
            if((! color.equals(ErrorStatus.KEY_FOBIDDEN)) && (! color.equals(ErrorStatus.NOT_FOUND)))
                results.add(new Tuple2<String, Integer>(key.concat("\tcolor\t").concat(color), 1));

            String price = FieldProcessor.process("price", productSearcher.search(productIds.getString(i),"price").get(0));
            if((! price.equals(ErrorStatus.KEY_FOBIDDEN)) && (! price.equals(ErrorStatus.NOT_FOUND))
                    && (! subcategory.equals(ErrorStatus.KEY_FOBIDDEN)) && (! subcategory.equals(ErrorStatus.NOT_FOUND)))
                results.add(new Tuple2<String, Integer>(key.concat("\tprice\t").concat(subcategory).concat("-").concat(price), 1));

            ArrayList<String> cuttingList = productSearcher.search(productIds.getString(i),"cutting");

            for(int j = 0; j < cuttingList.size(); j++){
                if((!cuttingList.get(j).equals(ErrorStatus.KEY_FOBIDDEN)) && (!cuttingList.get(j).equals(ErrorStatus.NOT_FOUND))
                        && (! subcategory.equals(ErrorStatus.KEY_FOBIDDEN)) && (! subcategory.equals(ErrorStatus.NOT_FOUND)))
                    results.add(new Tuple2<String, Integer>(key.concat("\tcutting\t").concat(subcategory).concat("-").concat(cuttingList.get(j)), 1));
            }

        }
        return results;
    }
}
