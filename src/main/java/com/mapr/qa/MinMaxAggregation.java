package com.mapr.qa;

import com.mapr.db.Admin;
import com.mapr.db.MapRDB;
import com.mapr.fs.ShimLoader;
import com.mapr.ojai.store.impl.OjaiOptions;
import org.ojai.Document;
import org.ojai.DocumentConstants;
import org.ojai.json.Json;
import org.ojai.store.*;
import org.ojai.types.OTimestamp;

import java.util.*;

public class MinMaxAggregation {

    final static String [] wellsIdArr = new String[] {"a", "b", "c", "d", "e", "f", "g","h"};
    final static String [] logIdArr = new String[] {"i","j","k","l","m","n","o","p"};
    private static Connection mConnection = null;
    public static final String CONNECTION_URL = "ojai:mapr:";
    private static Driver mDriver = null;
    private static final String mTableName = "/ATS-VOLUME-1578530753507-QueryMultiIndex" +
            "/com.mapr.qa.jsondb.tests.secondaryindex.query.QueryMultiIndex" +
            "/testQueryMultiIdxAscDescOnIdxNonCovWithAndMatchesProjectOnAnyFunctional0";

    static int numThreads = 100;
    static int numTimes = 10000000;
    static int numTags = 50;
    int lLimit = 1;
    final String processedTime = "processed_time";
    final String tags = "tags";

    final String ptStart = "2020-01-09T00:46:38.738Z";
    final String ptEnd = "2020-01-09T06:30:02.280Z";
    final String pt_idx_asc = "pt_idx_asc";
    final String pt_idx_desc = "pt_idx_desc";
    final String pt_idx_asc_wellid_logid = "pt_idx_asc_wellid_logid";
    final String pt_idx_desc_wellid_logid = "pt_idx_desc_wellid_logid";

    final String wellid_logid_pt_idx_asc = "wellid_logid_pt_idx_asc";
    final String wellid_logid_pt_idx_desc = "wellid_logid_pt_idx_desc";
    final String wellid_logid_pt_idx_asc_tags_included = "wellid_logid_pt_idx_asc_tags_included";
    final String wellIdLogId = "wellsId_logId";


    static {
        mConnection = DriverManager.getConnection(CONNECTION_URL);
        mDriver = mConnection.getDriver();
        ShimLoader.load();
    }

    public static void main(String[] args) {
        try {
            //createAndLoadTable
            //lminMinMaxAggregation.createAndLoadTable();
            MinMaxAggregation lminMinMaxAggregation = new MinMaxAggregation();

    //        lminMinMaxAggregation.findMinMaxWithParallelQueries();
            lminMinMaxAggregation.findMinMaxWithSingleQueryAndHashMap();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    class TagMinMaxValue {
        OTimestamp processedTimeMin;

        public void setProcessedTimeMax(OTimestamp processedTimeMax) {
            this.processedTimeMax = processedTimeMax;
        }

        public void setMaxId(String maxId) {
            this.maxId = maxId;
        }

        OTimestamp processedTimeMax;
        String minId;
        String maxId;
        TagMinMaxValue(OTimestamp processedTimeMin, OTimestamp processedTimeMax, String minId, String maxId) {
            this.processedTimeMin = processedTimeMin;
            this.processedTimeMax = processedTimeMax;
            this.minId = minId;
            this.maxId = maxId;
        }
    }
    private void findMinMaxWithSingleQueryAndHashMap() {
        try {

            Map<String, TagMinMaxValue> tagsMinMaxMap = new HashMap<>();
            List<String> tagsList = new ArrayList<>();
            for(int i=0; i< numTags; i++){
                tagsList.add("tags.tag" + i);
            }
            for (int j = 0; j < numTimes; j++) {


                //entire range with Equality filter and tags exist
                Query lQuery = getMinMaxEqualityAndTagsExist(tagsList, j);


                try (DocumentStore lStore1 = mConnection.getStore(mTableName)) {
                    try (QueryResult lqs1 = lStore1.find(lQuery)) {
//                            mLOG.info(lqs1.getQueryPlan());
                        for (Document lRes : lqs1) {
                            // populate the tagsMap

                            Map tagsInDoc = lRes.getMap(tags);
                            tagsList.parallelStream().forEach((tag) -> {

                                //for each tag which exists in the result
                                if(lRes.getValue(tag) != null){

                                    //map doesnt have the tag, put the processed time as min and max
                                    if(!tagsMinMaxMap.containsKey(tag)) {
                                        tagsMinMaxMap.put(tag
                                                , new TagMinMaxValue(lRes.getTimestamp(processedTime)
                                                        ,lRes.getTimestamp(processedTime)
                                                        , lRes.getIdString()
                                                        , lRes.getIdString()));
                                    } else { //tag exists in the map, update the max processed time and id

                                        TagMinMaxValue tagMinMaxValueExist = tagsMinMaxMap.get(tag);

                                        tagMinMaxValueExist.setProcessedTimeMax(lRes.getTimestamp(processedTime));
                                        tagMinMaxValueExist.setMaxId(lRes.getIdString());
                                    }

                                }
                            });
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }


    private void findMinMaxWithParallelQueries() {


        try {

            for (int j = 0; j < numTimes; j++) {


                List<Pair<Query, Query>> queryList = new ArrayList<>();

                for (int i = j * numThreads; i < (j * numThreads) + numThreads; i++) {

                    //min max aggregation with matches filter
                    //queryList.add(getMinMaxMatches(i));

                    //min max aggregation with is equals filter
                    //queryList.add(getMinMaxEquality(i);

                    //min max aggregation with matches filter and tag exists
                    queryList.add(getMinMaxMatchesAndTagExists(i));
                }

                queryList.parallelStream().forEach((lQPair) -> {
                    try (DocumentStore lStore1 = mConnection.getStore(mTableName)) {
                        try (QueryResult lqs1 = lStore1.find(lQPair.getFirst())) {
//                            mLOG.info(lqs1.getQueryPlan());
                            for (Document lRes : lqs1) {
//                                mLOG.info(lRes.getIdString());
                            }
                        }
                        try (QueryResult lqs2 = lStore1.find(lQPair.getSecond())) {
                            for (Document lRes : lqs2) {
//                                mLOG.info(lRes.getIdString());
                            }
                        }
                    }
                });

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void createAndLoadTable()
            throws Exception {

        final String processedTime = "processed_time";
        final String receivedTime = "received_time";
        final String mod = "mod";
        final String tagsKey = "tag";
        final String idKey = "wellsId_logId";
        int numRows = 10000000;
        Random lRand = new Random();
        Map<String, Double> tagsMap1 = new HashMap<>();
        Map<String, Double> tagsMap2 = new HashMap<>();
        Double piVal = new Double(3.14);
        Double halfPercent = new Double(0.5);
        int numThreads = 100;
        final Admin lAdmin = MapRDB.newAdmin();

        //create table
        DocumentStore lStore  = lAdmin.createTable(
                MapRDB.newTableDescriptor().setPath(mTableName)
                , new String [] {"a", "b", "c", "d", "e", "f", "g","h"});
        lStore.close();


        try {
            //build tags sets
            for (int i = 0; i < numTags; i++) {
                if(i % 2 == 0)
                    tagsMap1.put(tagsKey + i, piVal);
                else
                    tagsMap2.put(tagsKey + i, halfPercent);
            }
            //generate data and load
            for(int j =0; j < (numRows/ numThreads); j++) {

                List<Document> docList = new ArrayList<>();

                for (int i = j * numThreads; i < (j * numThreads) + numThreads; i++) {
                    String id = new String(wellsIdArr[i % wellsIdArr.length])
                            + "_" + new String(logIdArr[i % logIdArr.length]);
                    Document ldoc = Json.newDocument();
                    ldoc.set(processedTime, new OTimestamp(System.currentTimeMillis()));
                    ldoc.set(receivedTime, new OTimestamp(System.currentTimeMillis()));
                    ldoc.set(mod, lRand.nextBoolean() == true ? 1 : 0);
                    ldoc.set(tags, lRand.nextBoolean() == true ? tagsMap1 : tagsMap2);
                    ldoc.set(idKey, id);
                    ldoc.setId(id + "_" + System.currentTimeMillis());
                    docList.add(ldoc);
                }


                docList.parallelStream().forEach((lDoc) -> {
                    try (DocumentStore lStore1 = mConnection.getStore(mTableName)) {
                        lStore1.insertOrReplace(lDoc);
                    }
                });
            }
        } finally {
            if(lStore != null)
                lStore.close();
        }
    }

    private  Pair<Query, Query> getMinMaxMatches(int i)
            throws Exception {


                String idPred = new String(wellsIdArr[i % wellsIdArr.length])
                        + "_" + new String(logIdArr[i % logIdArr.length]);


                //matches
                QueryCondition lCondMatches = mDriver
                        .newCondition()
                        .and()
                        .is(processedTime
                                , QueryCondition.Op.GREATER_OR_EQUAL
                                , OTimestamp.parse(ptStart))
                        .is(processedTime
                                , QueryCondition.Op.LESS_OR_EQUAL
                                , OTimestamp.parse(ptEnd))
                        .matches(DocumentConstants.ID_KEY, idPred)
                        .close()
                        .build();
                Query lQueryAscMatches = mDriver.newQuery();
                lQueryAscMatches.select(DocumentConstants.ID_KEY, processedTime)
                        .where(lCondMatches)
                        .orderBy(processedTime, SortOrder.ASC)
                        .limit(lLimit)
                        .setOption(OjaiOptions.OPTION_USE_INDEX, pt_idx_asc)
                        .build();
                Query lQueryDescMatches = mDriver.newQuery();
                lQueryDescMatches.select(DocumentConstants.ID_KEY, processedTime)
                        .where(lCondMatches)
                        .orderBy(processedTime, SortOrder.DESC)
                        .limit(lLimit)
                        .setOption(OjaiOptions.OPTION_USE_INDEX, pt_idx_desc)
                        .build();
                return new Pair<>(lQueryAscMatches, lQueryDescMatches);
    }

    private Pair<Query, Query> getMinMaxEquality(int i){


                String idPred = new String(wellsIdArr[i % wellsIdArr.length])
                        + "_" + new String(logIdArr[i % logIdArr.length]);

                //eq range pruning
                QueryCondition lCondEq = mDriver
                        .newCondition()
                        .and()
                        .is(processedTime
                                , QueryCondition.Op.GREATER_OR_EQUAL
                                , OTimestamp.parse(ptStart))
                        .is(processedTime
                                , QueryCondition.Op.LESS_OR_EQUAL
                                , OTimestamp.parse(ptEnd))
                        .is(wellIdLogId, QueryCondition.Op.EQUAL, idPred)
                        .close()
                        .build();

                Query lQueryAscEq = mDriver.newQuery();
                lQueryAscEq.select(DocumentConstants.ID_KEY, processedTime)
                        .where(lCondEq)
                        .orderBy(processedTime, SortOrder.ASC)
                        .limit(lLimit)
                        .setOption(OjaiOptions.OPTION_USE_INDEX, wellid_logid_pt_idx_asc)
                        .build();

                Query lQueryDescEq = mDriver.newQuery();
                lQueryDescEq.select(DocumentConstants.ID_KEY, processedTime)
                        .where(lCondEq)
                        .orderBy(processedTime, SortOrder.DESC)
                        .limit(lLimit)
                        .setOption(OjaiOptions.OPTION_USE_INDEX, wellid_logid_pt_idx_desc)
                        .build();

               return new Pair<>(lQueryAscEq, lQueryDescEq);

    }

    private Pair<Query, Query> getMinMaxMatchesAndTagExists(int i) {

        String idPred = new String(wellsIdArr[i % wellsIdArr.length])
                + "_" + new String(logIdArr[i % logIdArr.length]);


        String tagName = "tags.tag" + i % numTags;

        //matches and tag exists
        QueryCondition lCondMatchesWithTagExists = mDriver
                .newCondition()
                .and()
                .is(processedTime
                        , QueryCondition.Op.GREATER_OR_EQUAL
                        , OTimestamp.parse(ptStart))
                .is(processedTime
                        , QueryCondition.Op.LESS_OR_EQUAL
                        , OTimestamp.parse(ptEnd))
                .matches(DocumentConstants.ID_KEY, idPred)
                .exists(tagName)
                .close()
                .build();

        Query lQueryAscMatchesAndTagExists = mDriver.newQuery();
        lQueryAscMatchesAndTagExists.select(DocumentConstants.ID_KEY, processedTime, tagName)
                .where(lCondMatchesWithTagExists)
                .orderBy(processedTime, SortOrder.ASC)
                .limit(lLimit)
                .setOption(OjaiOptions.OPTION_USE_INDEX, pt_idx_asc)
                .build();

        Query lQueryDescMatchesAndTagExists = mDriver.newQuery();
        lQueryDescMatchesAndTagExists.select(DocumentConstants.ID_KEY, processedTime, tagName)
                .where(lCondMatchesWithTagExists)
                .orderBy(processedTime, SortOrder.DESC)
                .limit(lLimit)
                .setOption(OjaiOptions.OPTION_USE_INDEX, pt_idx_desc)
                .build();

        return new Pair<>(lQueryAscMatchesAndTagExists, lQueryDescMatchesAndTagExists);
    }


    private Query getMinMaxEqualityAndTagsExist(List<String> tagsList, int i){


        String idPred = new String(wellsIdArr[i % wellsIdArr.length])
                + "_" + new String(logIdArr[i % logIdArr.length]);

        //eq range pruning with or on all tags
        QueryCondition lCondEqAndTagsExist = mDriver
                .newCondition()
                .and()
                .is(processedTime
                        , QueryCondition.Op.GREATER_OR_EQUAL
                        , OTimestamp.parse(ptStart))
                .is(processedTime
                        , QueryCondition.Op.LESS_OR_EQUAL
                        , OTimestamp.parse(ptEnd))
                .is(wellIdLogId, QueryCondition.Op.EQUAL, idPred);

        lCondEqAndTagsExist.or();
        for(String tag: tagsList)
            lCondEqAndTagsExist.exists(tag);

        lCondEqAndTagsExist.close()
                .close()
                .build();

        Query lQueryAscEqAndTagsExist = mDriver.newQuery();
        lQueryAscEqAndTagsExist.select(DocumentConstants.ID_KEY, processedTime, tags)
                .where(lCondEqAndTagsExist)
                .orderBy(processedTime, SortOrder.ASC)
                .limit(lLimit)
                .setOption(OjaiOptions.OPTION_USE_INDEX, wellid_logid_pt_idx_asc_tags_included)
                .build();


        return lQueryAscEqAndTagsExist;

    }
}
