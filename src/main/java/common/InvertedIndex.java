package common;

import org.json.JSONException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Collections;
import java.util.Map;

/**
 * Created by yishan on 3/8/16.
 */
public class InvertedIndex extends ProductSearcher {

    private HashMap<String, HashMap<String, ArrayList<String>>> invertedIndexMap = new HashMap<String, HashMap<String, ArrayList<String>>>();
    private HashMap<String, HashMap<String, Double>> elementDefaultDistributionMap;

    public InvertedIndex(ConfFromProperties confFromProperties) throws JSONException{
        super(confFromProperties);
        this.buildInvertedIndex();
        this.sortInvertedIndex();
        this.elementDefaultDistribution();
    }
    private void buildInvertedIndex(){

        ArrayList<String> productIds = new ArrayList<String>(this.productMap.keySet());
        for(int i = 0 ; i < this.fieldList.length; i++){
            String field = this.fieldList[i];
            HashMap<String, ArrayList<String>> fieldHashMap = new HashMap<String, ArrayList<String>>();
            for(int productIDIndex = 0; productIDIndex < productIds.size(); productIDIndex++){
                String productId = productIds.get(productIDIndex);
                ArrayList<String> elements = this.search(productId, field);
                ArrayList<String> elementsAfterProcess = new ArrayList<String>();
                if(field == "cutting" || field == "price"){
                    for(int index = 0 ; index < elements.size(); index++){
                        elementsAfterProcess.add(this.search(productId, "subcategory").get(0).concat("-").concat(FieldProcessor.process(field, elements.get(index))));
                    }
                }
                else{
                    for(int index = 0 ; index < elements.size(); index++){
                        elementsAfterProcess.add(FieldProcessor.process(field, elements.get(index)));
                    }
                }


                for(int j = 0; j < elementsAfterProcess.size(); j++){
                    String element = elementsAfterProcess.get(j);
                    if(!element.equals(ErrorStatus.KEY_FOBIDDEN) && !element.equals(ErrorStatus.NOT_FOUND)){
                        if(fieldHashMap.get(element)==null){
                            fieldHashMap.put(element, new ArrayList<String>());
                        }
                        fieldHashMap.get(element).add(productId);
                    }
                }
            }
            invertedIndexMap.put(this.fieldList[i], fieldHashMap);
        }
    }
    private void sortInvertedIndex(){

        for(int i = 0; i < this.fieldList.length; i++){
            String field = this.fieldList[i];
            ArrayList<String> elementList = new ArrayList<String>(this.invertedIndexMap.get(field).keySet());
            for(int j = 0; j < elementList.size(); j++){
                String element = elementList.get(j);
                Collections.sort(this.invertedIndexMap.get(field).get(element));
            }
        }
    }


    private void elementDefaultDistribution(){

        HashMap<String, HashMap<String, Integer>> productInfoCount = new HashMap<String, HashMap<String, Integer>>();

        ArrayList<String> fieldList = new ArrayList<String>(this.invertedIndexMap.keySet());
        for(int i = 0; i < fieldList.size(); i++){
            String field = fieldList.get(i);
            ArrayList<String> elementList = new ArrayList<String>(this.invertedIndexMap.get(field).keySet());

            HashMap<String, Integer> elementCountMap = new HashMap<String, Integer>();
            for(int j = 0; j < elementList.size(); j++){
                String element = elementList.get(j);
                elementCountMap.put(elementList.get(j), this.invertedIndexMap.get(field).get(element).size());
            }
            productInfoCount.put(field, elementCountMap);
        }
        this.elementDefaultDistributionMap = this.elementUserDistribution(productInfoCount);
    }

    private HashMap<String, HashMap<String, Double>>
            elementUserDistribution(HashMap<String, HashMap<String, Integer>> userInfoCount){

        HashMap<String, HashMap<String, Double>> elementUserDistributionMap = new HashMap<String, HashMap<String, Double>>();

        ArrayList<String> fieldList = new ArrayList<String>(userInfoCount.keySet());
        for(int i = 0; i < fieldList.size(); i++){
            String field = fieldList.get(i);
            ArrayList<String> elementList = new ArrayList<String>(userInfoCount.get(field).keySet());

            HashMap<String, Double> elementDistributionMap = new HashMap<String, Double>();
            int total = 0;
            for(int j = 0; j < elementList.size(); j++){
                String element = elementList.get(j);
                total += userInfoCount.get(field).get(element);
            }
            for(int j = 0; j < elementList.size(); j++){
                String element = elementList.get(j);
                elementDistributionMap.put(element, ((double) userInfoCount.get(field).get(element))/total);
            }

            elementUserDistributionMap.put(field, elementDistributionMap);

        }

        return elementUserDistributionMap;
    }

    private Double compareFieldDistance(String field, HashMap<String, Double> elementDistributionMap){
        HashMap<String, Double> elementDeafultDistributionMap = this.elementDefaultDistributionMap.get(field);
        ArrayList<String> elementList = new ArrayList<String>(elementDeafultDistributionMap.keySet());
        Double distance = new Double(0);
        for(int i = 0; i < elementList.size(); i++){
            String element = elementList.get(i);
            Double userDistributionValue = elementDistributionMap.get(element);
            if(userDistributionValue==null){
                userDistributionValue = new Double(0);
            }
            Double defaultDistributionValue = elementDeafultDistributionMap.get(element);
            distance += Math.abs(userDistributionValue-defaultDistributionValue);
        }
        return distance;
    }

    public ArrayList<String> invertedSearchCombine(HashMap<String, ArrayList<String>> filter){

        ArrayList<String> fields = new ArrayList<String>(filter.keySet());
        ArrayList<String> productIds = this.invertedIndexMap.get(fields.get(0)).get(filter.get(fields.get(0)).get(0).split("_")[0]);
        for(int i = 0 ; i < fields.size(); i++){
            ArrayList<String> elementList = filter.get(fields.get(i));
            for(int j = 0; j < elementList.size(); j++){
                if(elementList.get(j).split("_")[1].equals("1")){
                    String element = elementList.get(j).split("_")[0];
                    productIds.retainAll(this.invertedIndexMap.get(fields.get(i)).get(element));
                }
                else
                    if(elementList.get(j).split("_")[1].equals("0")){
                        String element = elementList.get(j).split("_")[0];
                        ArrayList<String> productIdsPriority = new ArrayList<String>(productIds);
                        productIdsPriority.retainAll(this.invertedIndexMap.get(fields.get(i)).get(element));
                        productIds.removeAll(productIdsPriority);
                        productIdsPriority.addAll(productIds);
                        productIds = new ArrayList<String>(productIdsPriority);
                }
            }
        }
        return productIds;
    }
    public ArrayList<String> getEssentialField(HashMap<String, HashMap<String, Integer>> userInfo){

        ArrayList<String> essentialFieldList = new ArrayList<String>();
        HashMap<String, HashMap<String, Double>> elementUserDistributionMap =  this.elementUserDistribution(userInfo);
        ArrayList<String> fieldList = new ArrayList<String>(elementUserDistributionMap.keySet());
        HashMap<String, Double> distanceMap = new HashMap<String, Double>();
        for(int i = 0 ; i < fieldList.size(); i++){
            String field = fieldList.get(i);
            Double distance = this.compareFieldDistance(field, elementUserDistributionMap.get(field));
            distanceMap.put(field, distance);
        }
        ArrayList<Map.Entry<String, Double>> distanceMapEntry = new ArrayList<Map.Entry<String, Double>>(distanceMap.entrySet());
        Collections.sort(distanceMapEntry,new MapSortByValueDouble());
        for(int i = 0; i < Math.min(3,distanceMapEntry.size()); i++){
            essentialFieldList.add(distanceMapEntry.get(i).getKey());
        }
        return essentialFieldList;
    }

    public HashMap<String, ArrayList<String>>
    getEssentialElement(HashMap<String, HashMap<String, Integer>> userInfo){
        HashMap<String, ArrayList<String>> essentialElements = new HashMap<String, ArrayList<String>>();
        ArrayList<String> essentialFieldList = this.getEssentialField(userInfo);

        for(int i = 0 ; i < essentialFieldList.size(); i++){
            ArrayList<String> essentialElementList = new ArrayList<String>();
            String field = essentialFieldList.get(i);
            HashMap<String, Integer> elementMap = userInfo.get(field);
            ArrayList<Map.Entry<String, Integer>> elementMapEntry = new ArrayList<Map.Entry<String, Integer>>(elementMap.entrySet());
            Collections.sort(elementMapEntry,new MapSortByValueInteger());
            for(int j = 0; j < Math.min(3,elementMapEntry.size()); j++){
                essentialElementList.add(elementMapEntry.get(j).getKey());
            }
            essentialElements.put(field,essentialElementList);
        }
        return essentialElements;
    }

    public ArrayList<HashMap<String, ArrayList<String>>>
    getEssentailFilters(HashMap<String, ArrayList<String>> essentialElements){
        ArrayList<HashMap<String, ArrayList<String>>> essentialRules = new ArrayList<HashMap<String, ArrayList<String>>>();
//        TODO
        ArrayList<String> cuttingElementList = new ArrayList<String>();
        if(essentialElements.keySet().contains("cutting")){
            for(int i = 0 ; i <essentialElements.get("cutting").size(); i++){
                cuttingElementList.add(essentialElements.get("cutting").get(i).concat("_0"));
            }
            essentialElements.remove("cutting");
        }
        ArrayList<Map.Entry<String, ArrayList<String>>> fieldEntryList =
                new  ArrayList<Map.Entry<String, ArrayList<String>>>(essentialElements.entrySet());
        essentialRules = this.autoCombine(fieldEntryList);
        if(!cuttingElementList.isEmpty()){
            for(int i = 0; i < essentialRules.size(); i++){
                essentialRules.get(i).put("cutting", cuttingElementList);
            }
        }

        return essentialRules;
    }
    private ArrayList<HashMap<String, ArrayList<String>>> autoCombine(ArrayList<Map.Entry<String, ArrayList<String>>> fieldEntryList){
        ArrayList<HashMap<String, ArrayList<String>>> essentialRules = new ArrayList<HashMap<String, ArrayList<String>>>();

        Map.Entry<String, ArrayList<String>> currentEntry = fieldEntryList.get(0);
        fieldEntryList.remove(0);

        if(fieldEntryList.size()==0){
            ArrayList<String> elementList = currentEntry.getValue();
            for(int i = 0; i<elementList.size(); i++){
                HashMap<String, ArrayList<String>> currentessentialRule = new HashMap<String, ArrayList<String>>();
                ArrayList<String> element = new ArrayList<String>();
                element.add(elementList.get(i).concat("_1"));
                currentessentialRule.put(currentEntry.getKey(), element);
                essentialRules.add(currentessentialRule);
            }
            return essentialRules;

        }
        else{
            ArrayList<HashMap<String, ArrayList<String>>> subEssentialRules = this.autoCombine(fieldEntryList);
            for(int i = 0 ; i < currentEntry.getValue().size(); i++){
                for(int j = 0; j < subEssentialRules.size(); j++){
                    HashMap<String, ArrayList<String>> currentessentialRule =
                            new HashMap<String, ArrayList<String>>(subEssentialRules.get(j));
                    ArrayList<String> element = new ArrayList<String>();
                    element.add(currentEntry.getValue().get(i).concat("_1"));
                    currentessentialRule.put(currentEntry.getKey(),element);
                    essentialRules.add(currentessentialRule);
                }
            }
            return essentialRules;
        }


    }
}
