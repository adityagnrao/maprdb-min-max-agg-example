/* Copyright (c) 2020 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.qa;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.ojai.Document;
import org.ojai.DocumentConstants;
import org.ojai.FieldPath;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.Query;
import org.ojai.store.QueryCondition;
import org.ojai.store.QueryResult;
import org.ojai.store.SortOrder;
import org.ojai.types.OTimestamp;

import com.mapr.ojai.store.impl.OjaiOptions;

public class AggregationFunctions {
  // TODO: Replace with the appropriate logging library used by the application
  private final static Logger LOGGER = Logger.getLogger(AggregationFunctions.class.getSimpleName());
  private static final FieldPath FIELDPATH_WELLID = FieldPath.parseFrom("wellsId");
  private static final FieldPath FIELDPATH_LOGID = FieldPath.parseFrom("logId");
  private static final FieldPath FIELDPATH_PROCESSEDTIME = FieldPath.parseFrom("processed_time");
  private static final FieldPath FIELDPATH_TAGS = FieldPath.parseFrom("tags");

  // name of the ascending/descending indexes, can be passed as the parameter to the functions
  protected static final String ascIndexName = "wellid_asc_logid_asc_pt_idx_asc_tags_included";
  protected static final String descIndexName = "wellid_asc_logid_asc_pt_idx_desc_tags_included";


  public static Map<String, Pair<Document, Document>> getDocumentsWithMinMaxForTags(
      final Connection ojaiConnection, final DocumentStore maprdbTable,
      final FieldPath[] projectedFields, final List<String> tagsList,
      final String refWellId, final String refLogId,
      final long refStartProcessingTimeInclusive, final long refStopProcessingTimeInclusive) {

    // scan the ascending index to find the min processed time for the TAGS_KEY
    final Map<String, String> tag2IdMinMap = findIdsForTags(ojaiConnection, maprdbTable, tagsList, refWellId,
        refLogId, refStartProcessingTimeInclusive, refStopProcessingTimeInclusive, SortOrder.ASC, ascIndexName);

    // scan the descending index to find the max processed time for the TAGS_KEY
    final Map<String, String> tag2IdMaxMap = findIdsForTags(ojaiConnection, maprdbTable, tagsList, refWellId,
        refLogId, refStartProcessingTimeInclusive, refStopProcessingTimeInclusive, SortOrder.DESC, descIndexName);

    final Set<String> idsToFetch = new HashSet<>();
    // add all IDs to a set so that we fetch them only once
    idsToFetch.addAll(tag2IdMinMap.values()); 
    idsToFetch.addAll(tag2IdMaxMap.values());
    
    // fetch the final documents from primary table, this is the join step
    final Map<String, Document> idsToDocumentMap = new HashMap<>();
    for (String docId : idsToFetch) {
      final Document resultDocument = maprdbTable.findById(docId, projectedFields);
      idsToDocumentMap.put(docId, resultDocument);
    }
    
    // build the result
    Map<String, Pair<Document, Document>> resultMap = new HashMap<>();
    for (String tagNumber : tagsList) {
      final String minId = tag2IdMinMap.get(tagNumber);
      final String maxId = tag2IdMaxMap.get(tagNumber);
      final Pair<Document, Document> minMaxDocsForTag = new Pair<>(idsToDocumentMap.get(minId), idsToDocumentMap.get(maxId));
      resultMap.put(tagNumber, minMaxDocsForTag);
    }
    
    return resultMap;
  }

  private static final Map<String, String> findIdsForTags(
      final Connection ojaiConnection, final DocumentStore maprdbTable,
      final List<String> tagsList, final String refWellId, final String refLogId,
      final long refStartProcessingTimeInclusive, final long refStopProcessingTimeInclusive,
      final SortOrder sortOrder, final String indexToUse) {

    final QueryCondition indexScanCondition = ojaiConnection
        .newCondition()
          .and()
            .is(FIELDPATH_WELLID, QueryCondition.Op.EQUAL, refWellId)
            .is(FIELDPATH_LOGID, QueryCondition.Op.EQUAL, refLogId)
            .is(FIELDPATH_PROCESSEDTIME, QueryCondition.Op.GREATER_OR_EQUAL, new OTimestamp(refStartProcessingTimeInclusive))
            .is(FIELDPATH_PROCESSEDTIME, QueryCondition.Op.LESS_OR_EQUAL, new OTimestamp(refStopProcessingTimeInclusive))
          .close()
        .build();

    final Query indexScanQuery = ojaiConnection
        .newQuery()
          .select(DocumentConstants.ID_FIELD, FIELDPATH_TAGS)
          .where(indexScanCondition)
          .orderBy(FIELDPATH_PROCESSEDTIME, sortOrder)
          .setOption(OjaiOptions.OPTION_USE_INDEX, indexToUse)
        .build();

    int numRowsScanned = 0;
    final Set<String> tagsSet = new HashSet<>(tagsList);
    final Map<String, String> tag2IdMap = new HashMap<>();

    LOGGER.info(indexScanQuery.asJsonString());
    try (final QueryResult queryResult = maprdbTable.find(indexScanQuery)) {
      LOGGER.info(queryResult.getQueryPlan().asJsonString()); // TODO: change to debug

      for (final Document document : queryResult) {
        numRowsScanned++;
        // extract the TAGS_KEY MAP, so that lookup is faster in the next step
        final Map<String, Object> tagsMapInDocument = document.getMap(FIELDPATH_TAGS);

        // iterate over unseen TAGS_KEY to see if any of them exist in the current document
        for (final Iterator<String> tagItr = tagsSet.iterator(); tagItr.hasNext();) {
          final String tagNumber = (String) tagItr.next();
          if (tagsMapInDocument.containsKey(tagNumber)) {
            tag2IdMap.put(tagNumber, document.getIdString());
            tagItr.remove(); // found the tag, no need to look further so remove it from the set
          }          
        }
        
        if (tagsSet.isEmpty()) {
          // all TAGS_KEY are seen, terminate the query and return
          LOGGER.info("Found all TAGS_KEY, number of fetched document: " + numRowsScanned);  // TODO: change to debug
          break;
        }
      }

      if(!tagsSet.isEmpty()) {
        LOGGER.info("Found some of the TAGS_KEY, number of fetched document: " + numRowsScanned);
      }

    }
    
    return tag2IdMap;          
  }

}
