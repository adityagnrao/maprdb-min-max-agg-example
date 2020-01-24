package com.mapr.qa;

import com.mapr.db.Admin;
import com.mapr.db.MapRDB;
import com.mapr.fs.ShimLoader;
import org.apache.commons.cli.*;
import org.ojai.Document;
import org.ojai.DocumentConstants;
import org.ojai.FieldPath;
import org.ojai.json.Json;
import org.ojai.store.*;
import org.ojai.types.OTimestamp;


import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import static java.lang.System.exit;

public class MinMaxAggregation {
    private static final Logger LOG = Logger.getLogger(AggregationFunctions.class.getSimpleName());
    private static final String [] wellsIdArr = new String[] {"a", "b", "c", "d", "e", "f", "g","h"};
    private static final String [] logIdArr = new String[] {"i","j","k","l","m","n","o","p"};
    private static Connection mConnection = null;
    public static final String CONNECTION_URL = "ojai:mapr:";
    private static String mTableName = "/MinMaxAggregationTable";
    private static final String TABLENAME = "table";
    private static final String CREATE = "create";
    private static final String AGGREGATE = "aggregate";
    private static final String LOAD = "load";
    private static final String FIELDS = "fields";
    private static final String PT_START = "pt_start";
    private static final String PT_END = "pt_end";

    private long startTime;
    private int numRows = 10000000;
    private int numTags = 50;
    private int scanRange = 100000;
    private static final String PROCESSED_TIME = "processed_time";
    private static final String TAGS_KEY = "tags";
    private static final String TAG_KEY = "tag";
    private static final String DOT = ".";
    private static final String DASH = "_";

    private static final String WELLS_ID_KEY = "wellsId";
    private static final String LOG_ID_KEY = "logId";
    private static final String NUM_ROWS = "numRows";
    private static final String NUM_TAGS = "numTags";
    private static final String SCAN_RANGE = "scanRange";

    String wellsId = wellsIdArr[0];
    String logId = logIdArr[0];

    private static Options createOptions;
    private static Options loadOptions;
    private static Options aggregateOptions;
    List<FieldPath> mProjectedFieldsList;
    String ptStart = "2020-01-09T00:46:38.738Z";
    String ptEnd = "2020-01-09T06:30:02.280Z";
    private static CmdEnum mCmd = CmdEnum.LOAD;

    private enum CmdEnum {
        CREATE, LOAD, AGGREGATE
    }
    static {
        mConnection = DriverManager.getConnection(CONNECTION_URL);
        ShimLoader.load();
    }

    protected MinMaxAggregation (){
        createOptions = new Options();
        loadOptions = new Options();
        aggregateOptions = new Options();
        startTime = System.nanoTime();
        mProjectedFieldsList = new ArrayList<>();
    }

    public static void main(String[] args) {
        try {
            MinMaxAggregation lminMinMaxAggregation = new MinMaxAggregation();
            //parse args
            if(lminMinMaxAggregation.parseArgs(args)) {
                switch(mCmd) {
                    case LOAD:
                        //loadTable
                        lminMinMaxAggregation.loadTable();
                        break;
                    case CREATE:
                        lminMinMaxAggregation.createTable();
                        break;
                    case AGGREGATE:
                        lminMinMaxAggregation.findMinMaxWithSingleQueryAndHashMap(1);
                        break;
                }
            } else {
                printHelp("Please Check the Arguments passed");
                exit(-1);
            }
            System.out.println("Time (ms) : "
                    + ((double)(System.nanoTime() - lminMinMaxAggregation.startTime))/1000000);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void createIndices() throws Exception{
        String createAscIdxCmd = "maprcli table index add" +
                " -path " + mTableName +
                " -index " + AggregationFunctions.ascIndexName +
                " -indexedfields "+ WELLS_ID_KEY +","+ LOG_ID_KEY +","+PROCESSED_TIME+":asc" +
                " -includedfields " + TAGS_KEY + " -json";

        String createDescIdxCmd = "maprcli table index add " +
                " -path " + mTableName +
                " -index " + AggregationFunctions.descIndexName +
                " -indexedfields "+ WELLS_ID_KEY +","+ LOG_ID_KEY +","+PROCESSED_TIME+":desc" +
                " -includedfields " + TAGS_KEY + " -json";

        exec(createAscIdxCmd);
        exec(createDescIdxCmd);
    }

    private void exec(String cmd) {
        LOG.info(cmd);
        StringBuffer output = new StringBuffer();
        String line;
        Process p;
        InputStream is;
        InputStreamReader isr;
        BufferedReader br;
        try {
            p = Runtime.getRuntime().exec(cmd);
            is = p.getInputStream();
            isr = new InputStreamReader(is);
            br = new BufferedReader(isr);

            while ((line = br.readLine()) != null) {
                output.append(line + "\n");
            }
            p.waitFor();
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.info(output.toString());
    }

    private void createTable() throws Exception{
        Admin lAdmin = null;
        //create table
        DocumentStore lStore = null;

        try {
            lAdmin = MapRDB.newAdmin();

            //create table
            lStore = lAdmin.createTable(
                    MapRDB.newTableDescriptor().setPath(mTableName)
                    , wellsIdArr);
        } catch (com.mapr.db.exceptions.TableExistsException e) {
          //expected exception
        } finally {

            if(lStore != null)
                lStore.close();

            if(lAdmin != null)
                lAdmin.close();
        }

        //create indices
        createIndices();

        //print table info
        printTableInfo();

        //print index list
        printIndexList();


    }

    private void printTableInfo() {
        String tableInfoCmd = "maprcli table info -path " + mTableName + " -json";
        exec(tableInfoCmd);
    }

    private void printIndexList() {
        String tableIdxList = "maprcli table index list -path " + mTableName + " -json -refreshnow true";
        exec(tableIdxList);
    }

    private boolean parseArgs(String args[]) throws Exception {
        boolean parsingComplete = false;
        CommandLine parser = null;
        initOptions();

        try {
            if (args.length == 0
                    || args[0].equalsIgnoreCase("-help")
                    || args[0].equalsIgnoreCase("-h")
                    || args[0].equalsIgnoreCase("--h")
            ) {

                printHelp(null);
                exit(0);

            } else if (args[0].equalsIgnoreCase(CREATE)){
                mCmd = CmdEnum.CREATE;
                parser = new GnuParser().parse(createOptions, args);
            } else if(args[0].equalsIgnoreCase(LOAD)) {
                mCmd = CmdEnum.LOAD;
                parser = new GnuParser().parse(loadOptions, args);
            } else if(args[0].equalsIgnoreCase(AGGREGATE)) {
                mCmd = CmdEnum.AGGREGATE;
                parser = new GnuParser().parse(aggregateOptions, args);
            } else {
                printHelp("Invalid arguments provided");
                exit(-1);
            }

            if(parser.hasOption(TABLENAME)) {
                mTableName = parser.getOptionValue(TABLENAME);
                if (mTableName != null && !Pattern.matches("/.+", mTableName)) {
                    printHelp("Invalid table name provided");
                    exit(-1);
                }

                if(parser.hasOption(PT_START))
                    ptStart = parser.getOptionValue(PT_START);

                if(parser.hasOption(PT_END))
                    ptEnd = parser.getOptionValue(PT_END);

                if(parser.hasOption(FIELDS))
                {
                    for(String field : parser.getOptionValue(FIELDS).split(","))
                        mProjectedFieldsList.add(FieldPath.parseFrom(field));
                }

                if(parser.hasOption(WELLS_ID_KEY))
                    wellsId = parser.getOptionValue(WELLS_ID_KEY);

                if(parser.hasOption(LOG_ID_KEY))
                    logId = parser.getOptionValue(LOG_ID_KEY);

                if(parser.hasOption(NUM_ROWS))
                    numRows = Integer.parseInt(parser.getOptionValue(NUM_ROWS));

                if(parser.hasOption(NUM_TAGS))
                    numTags = Integer.parseInt(parser.getOptionValue(NUM_TAGS));

                if(parser.hasOption(SCAN_RANGE))
                    scanRange = Integer.parseInt(parser.getOptionValue(SCAN_RANGE));

                parsingComplete = true;
            }
            else {
                printHelp("Please provide table name");
                exit(-1);
            }

        } catch (Exception e) {
            printHelp(e.getMessage());
            exit(-1);
        }


        return parsingComplete;
    }

    private void initOptions() {
        Option optionTable = OptionBuilder
                .withArgName(TABLENAME)
                .hasArg()
                .withDescription("table name")
                .isRequired(true)
                .create(TABLENAME);
        createOptions.addOption(optionTable);


        Option optionFields = OptionBuilder
                .withArgName(FIELDS)
                .hasArg()
                .withDescription("Fields to project after aggregation: [_id,processed_time,TAGS_KEY.tag1,TAGS_KEY,tag2]")
                .isRequired(false)
                .create(FIELDS);

        Option optionPTStart = OptionBuilder
                .withArgName(PT_START)
                .hasArg()
                .withDescription("Start Timestamp")
                .isRequired(true)
                .create(PT_START);

        Option optionPTEnd = OptionBuilder
                .withArgName(PT_END)
                .hasArg()
                .withDescription("End Timestamp")
                .isRequired(true)
                .create(PT_END);

        Option optionWellsId = OptionBuilder
                .withArgName(WELLS_ID_KEY)
                .hasArg()
                .withDescription("wellsId : " + Arrays.toString(wellsIdArr))
                .isRequired(false)
                .create(WELLS_ID_KEY);

        Option optionlogId = OptionBuilder
                .withArgName(LOG_ID_KEY)
                .hasArg()
                .withDescription("logId : " + Arrays.toString(logIdArr))
                .isRequired(false)
                .create(LOG_ID_KEY);

        Option optionNumTags = OptionBuilder
                .withArgName(NUM_TAGS)
                .hasArg()
                .withDescription("number of rows to load : [ " + numTags + " ]" )
                .isRequired(false)
                .create(NUM_TAGS);

        aggregateOptions.addOption(optionTable);
        aggregateOptions.addOption(optionPTStart);
        aggregateOptions.addOption(optionPTEnd);
        aggregateOptions.addOption(optionFields);
        aggregateOptions.addOption(optionWellsId);
        aggregateOptions.addOption(optionlogId);
        aggregateOptions.addOption(optionNumTags);

        Option optionNumRows = OptionBuilder
                .withArgName(NUM_ROWS)
                .hasArg()
                .withDescription("number of rows to load : [ " + numRows + " ]" )
                .isRequired(false)
                .create(NUM_ROWS);

        Option optionScanRange = OptionBuilder
                .withArgName(SCAN_RANGE)
                .hasArg()
                .withDescription("number of rows to load before loading a row with all tags : [ " + scanRange + " ]" )
                .isRequired(false)
                .create(SCAN_RANGE);

        loadOptions.addOption(optionTable);
        loadOptions.addOption(optionNumRows);
        loadOptions.addOption(optionNumTags);
        loadOptions.addOption(optionScanRange);

    }

    private static void printHelp(String reasonIfError){
        new HelpFormatter().printHelp("MinMaxAggregation " + CREATE
                ,"create the table and indexes"
                , createOptions
                , reasonIfError
                , true);

        System.out.println();

        new HelpFormatter().printHelp("MinMaxAggregation " + LOAD
                ,"load the table with documents"
                , loadOptions
                , reasonIfError
                , true);

        System.out.println();

        new HelpFormatter().printHelp("MinMaxAggregation " + AGGREGATE
                ,"find the documents in the given processed_time range " +
                        "for given tag Id and wells Id " +
                        "with min and max processed_times for each tag"
                , aggregateOptions
                , reasonIfError
                , true);

        System.out.println();

    }



    private void findMinMaxWithSingleQueryAndHashMap(int lNumTimes) {
        try {

            if(mProjectedFieldsList.isEmpty()) {
                mProjectedFieldsList.add(FieldPath.parseFrom(PROCESSED_TIME));
                mProjectedFieldsList.add(FieldPath.parseFrom(DocumentConstants.ID_KEY));
                mProjectedFieldsList.add(FieldPath.parseFrom(TAGS_KEY + DOT + TAG_KEY + 1));
                mProjectedFieldsList.add(FieldPath.parseFrom(TAGS_KEY + DOT + TAG_KEY + 2));
            }

            List<String> tagsList = new ArrayList<>();
            for(int i=0; i< numTags; i++){
                tagsList.add(TAG_KEY + i);
            }
            for (int j = 0; j < lNumTimes; j++) {
                Map<String, Pair<Document, Document>> lResMap = AggregationFunctions.getDocumentsWithMinMaxForTags(
                        mConnection
                        , mConnection.getStore(mTableName)
                        , mProjectedFieldsList.toArray(new FieldPath[0])
                        ,  tagsList
                        , wellsId != null ? wellsId : wellsIdArr[j % wellsIdArr.length]
                        , logId != null ? logId : logIdArr[j % logIdArr.length]
                        , OTimestamp.parse(ptStart).toDate().getTime()
                        , OTimestamp.parse(ptEnd).toDate().getTime());

                for(Map.Entry<String, Pair<Document, Document>> entry : lResMap.entrySet()){
                    System.out.println("key : " + entry.getKey() + " " + entry.getValue());
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void loadTable()
            throws Exception {

        final String receivedTime = "received_time";
        final String mod = "mod";
        final String idKey = WELLS_ID_KEY + DASH + LOG_ID_KEY;
        Random lRand = new Random();
        List<Map<String, Double>> tagMapList = new ArrayList<>();
        int lNumTagMaps = 50;
        Double piVal = new Double(3.14);
        Double halfPercent = new Double(0.5);
        int numThreads = 100;

        //build TAGS_KEY sets
        Map<String, Double> tagsMap1 = new HashMap<>();
        for (int k = 0; k < numTags; k++) {
            tagsMap1.put(TAG_KEY + k, piVal);
        }
        for(int i=0; i <  lNumTagMaps - 1;i++) {
            Map<String, Double> tagsMap2 = new HashMap<>();
            for (int k = 0; k < lRand.nextInt(numTags - 1); k++) {
                if (k % 2 == 0)
                    tagsMap2.put(TAG_KEY + k, piVal);
                else
                    tagsMap2.put(TAG_KEY + k, halfPercent);
            }
            tagMapList.add(tagsMap2);
        }

        //generate data and load
        for(int j =0; j < (numRows/ numThreads); j++) {

            List<Document> docList = new ArrayList<>();

            for (int i = j * numThreads; i < (j * numThreads) + numThreads; i++) {
                String wellId = wellsIdArr[i % wellsIdArr.length];
                String logId = logIdArr[j % logIdArr.length];
                String id = wellId + DASH + logId;
                Document ldoc = Json.newDocument();
                ldoc.set(PROCESSED_TIME, new OTimestamp(System.currentTimeMillis()))
                        .set(receivedTime, new OTimestamp(System.currentTimeMillis()))
                        .set(mod, lRand.nextBoolean() == true ? 1 : 0)
                        .set(TAGS_KEY, i % scanRange == 0 ? tagsMap1 : tagMapList.get(lRand.nextInt(tagMapList.size())))
                        .set(idKey, id)
                        .set(WELLS_ID_KEY, wellId)
                        .set(LOG_ID_KEY, logId)
                        .setId(id + DASH + System.currentTimeMillis());
                docList.add(ldoc);
            }


            docList.parallelStream().forEach((lDoc) -> {
                try (DocumentStore lStore1 = mConnection.getStore(mTableName)) {
                    lStore1.insertOrReplace(lDoc);
                }
            });
        }
    }
}
