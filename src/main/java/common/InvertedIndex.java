package common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Collections;
/**
 * Created by yishan on 3/8/16.
 */
public class InvertedIndex extends ProductSearcher {

    private HashMap<String, HashMap<String, ArrayList<String>>> invertedIndexMap = new HashMap<String, HashMap<String, ArrayList<String>>>();
    private HashMap<String, HashMap<String, Double>> elementDefaultDistributionMap;

    public InvertedIndex(ConfFromProperties confFromProperties){
        super(confFromProperties);
        this.buildInvertedIndex();
        this.sortInvertedIndex();
        this.elementDefaultDistribution();
    }
    private void buildInvertedIndex(){

        ArrayList<String> productIds = (ArrayList<String>) this.productMap.keySet();
        for(int i = 0 ; i < this.fieldList.length; i++){
            String field = this.fieldList[i];
            HashMap<String, ArrayList<String>> fieldHashMap = new HashMap<String, ArrayList<String>>();
            for(int productIDIndex = 0; productIDIndex < productIds.size(); i++){
                String productId = productIds.get(productIDIndex);
                ArrayList<String> elements = this.search(productId, field);
                for(int j = 0; j < elements.size(); j++){
                    String element = elements.get(j);
                    if(fieldHashMap.get(element)==null){
                        fieldHashMap.put(element, new ArrayList<String>());
                    }
                    if(!element.equals(ErrorStatus.KEY_FOBIDDEN) && !element.equals(ErrorStatus.NOT_FOUND)){
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
            ArrayList<String> elementList = (ArrayList<String>) this.invertedIndexMap.get(field).keySet();
            for(int j = 0; j < elementList.size(); j++){
                String element = elementList.get(j);
                Collections.sort(this.invertedIndexMap.get(field).get(element));
            }
        }
    }


    private void elementDefaultDistribution(){

        HashMap<String, HashMap<String, Integer>> productInfoCount = new HashMap<String, HashMap<String, Integer>>();

        ArrayList<String> fieldList = (ArrayList<String>) this.invertedIndexMap.keySet();
        for(int i = 0; i < fieldList.size(); i++){
            String field = fieldList.get(i);
            ArrayList<String> elementList = (ArrayList<String>) this.invertedIndexMap.get(field).keySet();

            HashMap<String, Integer> elementCountMap = new HashMap<String, Integer>();
            for(int j = 0; j < elementList.size(); j++){
                String element = elementList.get(j);
                elementCountMap.put(elementList.get(i), this.invertedIndexMap.get(field).get(element).size());
            }
            productInfoCount.put(field, elementCountMap);
        }
        this.elementDefaultDistributionMap = this.elementUserDistribution(productInfoCount);
    }

    private HashMap<String, HashMap<String, Double>>
            elementUserDistribution(HashMap<String, HashMap<String, Integer>> userInfoCount){

        HashMap<String, HashMap<String, Double>> elementUserDistributionMap = new HashMap<String, HashMap<String, Double>>();

        ArrayList<String> fieldList = (ArrayList<String>) userInfoCount.keySet();
        for(int i = 0; i < fieldList.size(); i++){
            String field = fieldList.get(i);
            ArrayList<String> elementList = (ArrayList<String>) userInfoCount.get(field).keySet();

            HashMap<String, Double> elementDistributionMap = new HashMap<String, Double>();
            int total = 0;
            for(int j = 0; j < elementList.size(); j++){
                String element = elementList.get(j);
                total += this.invertedIndexMap.get(field).get(element).size();
            }
            for(int j = 0; j < elementList.size(); j++){
                String element = elementList.get(j);
                elementDistributionMap.put(elementList.get(i), new Double(this.invertedIndexMap.get(field).get(element).size())/total);
            }

            elementUserDistributionMap.put(field, elementDistributionMap);

        }

        return elementUserDistributionMap;
    }

    private Double compareFieldDistance(String field, HashMap<String, Double> elementDistributionMap){
        HashMap<String, Double> elementDeafultDistributionMap = this.elementDefaultDistributionMap.get(field);
        ArrayList<String> elementList = (ArrayList<String>) elementDeafultDistributionMap.keySet();
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

        ArrayList<String> fields = (ArrayList<String>) filter.keySet();
        ArrayList<String> productIds = this.invertedIndexMap.get(fields.get(0)).get(filter.get(fields.get(0)).get(0).split("_")[0]);
        for(int i = 0 ; i < fields.size(); i++){
            ArrayList<String> elementList = filter.get(fields.get(0));
            for(int j = 0; j < elementList.size(); j++){
                if(filter.get(fields.get(0)).get(j).split("_")[1].equals("1")){
                    productIds.retainAll(this.invertedIndexMap.get(fields.get(i)).get(filter.get(fields.get(i)).get(j)));
                }
                else{
                    ArrayList<String> productIdsPriority = (ArrayList<String>) productIds.clone();
                    productIdsPriority.retainAll(this.invertedIndexMap.get(fields.get(i)).get(filter.get(fields.get(i)).get(j)));
                    productIds.removeAll(productIdsPriority);
                    productIdsPriority.addAll(productIds);
                    productIds = (ArrayList<String>) productIdsPriority.clone();
                }
            }
        }
        return productIds;
    }
    public ArrayList<String> getEssentialField(HashMap<String, HashMap<String, Integer>> userInfo){

        ArrayList<String> essentialFieldList = new ArrayList<String>();
        HashMap<String, HashMap<String, Double>> elementUserDistributionMap =  this.elementUserDistribution(userInfo);
        ArrayList<String> fieldList = (ArrayList<String>) elementUserDistributionMap.keySet();
        HashMap<Double,String> distanceMap = new HashMap<Double,String>();
        for(int i = 0 ; i < fieldList.size(); i++){
            String field = fieldList.get(i);
            Double distance = this.compareFieldDistance(field, elementUserDistributionMap.get(field));
            distanceMap.put(distance,field);
        }
        ArrayList<Double> distanceList = (ArrayList<Double>) distanceMap.keySet();
        Collections.sort(distanceList);
        for(int i = 0 ; i < 3; i++){
            essentialFieldList.add(distanceMap.get(distanceList.get(i)));
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
            HashMap<String, Integer> elementList = userInfo.get(field);

        }
        return essentialElements;
    }

    public ArrayList<HashMap<String, ArrayList<String>>>
    getEssentailFilters(HashMap<String, ArrayList<String>> essentialElements){
        ArrayList<HashMap<String, ArrayList<String>>> essentialRules = new ArrayList<HashMap<String, ArrayList<String>>>();
//        TODO

        return essentialRules;
    }
}
