package common;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by yishan on 1/8/16.
 */
class ProductHashMap extends HashMap implements Serializable {
    private static Logger log = Logger.getLogger(ProductHashMap.class.getName());

    public ProductHashMap(){
        ArrayList<String> idArrayList = new ArrayList<String>();
        idArrayList.add(ErrorStatus.NOT_FOUND);
        this.put("id", idArrayList);

        ArrayList<String> priceArrayList = new ArrayList<String>();
        priceArrayList.add(ErrorStatus.NOT_FOUND);
        this.put("price", priceArrayList);

        ArrayList<String> brandArrayList = new ArrayList<String>();
        brandArrayList.add(ErrorStatus.NOT_FOUND);
        this.put("brand", brandArrayList);

        ArrayList<String> categoryArrayList = new ArrayList<String>();
        categoryArrayList.add(ErrorStatus.NOT_FOUND);
        this.put("category", categoryArrayList);

        ArrayList<String> subcategoryArrayList = new ArrayList<String>();
        subcategoryArrayList.add(ErrorStatus.NOT_FOUND);
        this.put("subcategory", subcategoryArrayList);

        ArrayList<String> colorArrayList = new ArrayList<String>();
        colorArrayList.add(ErrorStatus.NOT_FOUND);
        this.put("color", colorArrayList);

        ArrayList<String> patternArrayList = new ArrayList<String>();
        patternArrayList.add(ErrorStatus.NOT_FOUND);
        this.put("pattern", patternArrayList);

        ArrayList<String> cuttingArrayList = new ArrayList<String>();
        cuttingArrayList.add(ErrorStatus.NOT_FOUND);
        this.put("cutting", cuttingArrayList);
    }

    public ProductHashMap(ResultSet resultSet) throws SQLException, JSONException {
        ArrayList<String> idArrayList = new ArrayList<String>();
        idArrayList.add(resultSet.getString("id"));
        this.put("id", idArrayList);

        ArrayList<String> priceArrayList = new ArrayList<String>();
        priceArrayList.add(resultSet.getString("price"));
        this.put("price", priceArrayList);

        ArrayList<String> brandArrayList = new ArrayList<String>();
        brandArrayList.add(resultSet.getString("brand_id"));
        this.put("brand", brandArrayList);

        ArrayList<String> categoryArrayList = new ArrayList<String>();
        categoryArrayList.add(resultSet.getString("category"));
        this.put("category", categoryArrayList);

        ArrayList<String> subcategoryArrayList = new ArrayList<String>();
        subcategoryArrayList.add(resultSet.getString("sub_category"));
        this.put("subcategory", subcategoryArrayList);

        ArrayList<String> colorArrayList = new ArrayList<String>();
        colorArrayList.add(resultSet.getString("ex_color"));
        this.put("color", colorArrayList);

        JSONObject tags = new JSONObject(resultSet.getString("tags"));

        ArrayList<String> patternArrayList = new ArrayList<String>();
        try{
            JSONArray patternJsonArray = tags.getJSONArray("3");
            for(int i = 0; i < patternJsonArray.length(); i++){
                patternArrayList.add(patternJsonArray.getString(i));
            }
        }
        catch (Exception e){
            log.trace("The product {id} can't find pattern".replaceAll("\\{id\\}",((ArrayList<String>) this.get("id")).get(0)));
            patternArrayList.add(ErrorStatus.NOT_FOUND);
        }
        finally {
            this.put("pattern", patternArrayList);
        }


        ArrayList<String> cuttingArrayList = new ArrayList<String>();
        try{
            JSONArray cuttingJsonArray = tags.getJSONArray("5");
            for(int i = 0; i < cuttingJsonArray.length(); i++) {
                cuttingArrayList.add(cuttingJsonArray.getString(i));
            }
        }
        catch (Exception e){
            log.trace("The product {id} can't find cutting".replaceAll("\\{id\\}",((ArrayList<String>) this.get("id")).get(0)));
            cuttingArrayList.add(ErrorStatus.NOT_FOUND);
        }
        finally {
            this.put("cutting", cuttingArrayList);
        }

    }
}