package common;

import java.util.ArrayList;

/**
 * Created by yishan on 1/8/16.
 */
public class FieldProcessor {

    public static String process(String field, String value){
        if(field == "price"){
            return PriceClassfier.search(Integer.parseInt(value));
        }
        else if(field == "brand"){
            return "b".concat(Integer.toString(Integer.parseInt(value)));
        }
        else{
            return value;
        }
    }

    public static ArrayList<String> processPriceRange(ArrayList<String> priceRange){
        return PriceClassfier.search(priceRange);
    }

}

class PriceClassfier {
    private static int[] priceRange = {0, 3000, 5000, 8000, 12000, 20000, 100000000};
    public static String search(int price){
        for(int i = 0; i < PriceClassfier.priceRange.length; i++){
            if(price >= PriceClassfier.priceRange[i] && price < PriceClassfier.priceRange[i+1]){
                return "p".concat(Integer.toString(i));
            }
        }
        return ErrorStatus.NOT_FOUND;
    }
    public static ArrayList<String> search(ArrayList<String> priceRange){
        int min = Integer.parseInt(priceRange.get(0));
        int max = Integer.parseInt(priceRange.get(1));
        ArrayList<String> results = new ArrayList<String>();
        for(int i = 0; i < PriceClassfier.priceRange.length; i++){
            if(min <= PriceClassfier.priceRange[i] && max > PriceClassfier.priceRange[i]){
                results.add(Integer.toString(i));
            }
        }
        return results;
    }
}
