--Change these jar locations to point to the correct locations/version on your system.
REGISTER $JARFILES/mongo-java-driver-2.13.0.jar
REGISTER $JARFILES/mongo-hadoop-core-1.3.3-SNAPSHOT.jar
REGISTER $JARFILES/mongo-hadoop-pig-1.3.3-SNAPSHOT.jar
-- REGISTER /Users/tang/Projects/mongo-hadoop/examples/lm_posts/udf/datafu-1.2.0.jar
REGISTER '$LM_UDF/lm_udf.py' using org.apache.pig.scripting.jython.JythonScriptEngine as lm_udf;

-- define TransposeTupleToBag datafu.pig.util.TransposeTupleToBag();

posts = LOAD 'mongodb://$DB:$DB_PORT/localmeasure.posts' 
    USING com.mongodb.hadoop.pig.MongoLoader('id:chararray, post_time:chararray, secondary_venue_ids:chararray, kind:chararray', 'id') 
    AS (id:chararray, post_time:chararray, secondary_venue_ids:chararray, kind:chararray); 

places = LOAD 'mongodb://$DB:$DB_PORT/localmeasure.places' 
    USING com.mongodb.hadoop.pig.MongoLoader('id:chararray, name:chararray, merchant_id:chararray, venue_ids:chararray', 'id') 
    AS (id:chararray, name:chararray, merchant_id:chararray, venue_ids:chararray);

merchants = LOAD 'mongodb://$DB:$DB_PORT/localmeasure.merchants' 
    USING com.mongodb.hadoop.pig.MongoLoader('id, name, subscription', 'id');

merchants2 = FOREACH merchants GENERATE $0 AS id, $1 AS name, lm_udf.is_expired($2#'expires_at') AS expiry;
active_merchants = FILTER merchants2 BY expiry == 0;

active_places = JOIN places BY merchant_id, active_merchants BY id;

-- Strip off the leading [] chars in the venue_id array. This shouldn't be necessary with Mongo loader then flatten venue_ids to a row each.
active_split_places = FOREACH active_places GENERATE places::name AS place_name, active_merchants::id AS merchant_id,
                        FLATTEN(TOKENIZE(lm_udf.venue_id_strip(venue_ids))) AS venue_id;

-- Flatten teh posts collection similarly, TODO: create UDF's for all the date fields with a date_helper UDF
split_posts = FOREACH posts GENERATE id, kind, 
        SUBSTRING(id, 0, 2) AS source,
        CONCAT(SUBSTRING(post_time, 24, 28), SUBSTRING(post_time, 4, 7)) AS month,
        FLATTEN(TOKENIZE(lm_udf.venue_id_strip(secondary_venue_ids))) AS venue_id;

split_posts = FILTER split_posts BY month == '$MONTH';

places_posts_joined = JOIN active_split_places BY venue_id, split_posts BY venue_id;
places_posts_distinct = FOREACH places_posts_joined GENERATE active_split_places::merchant_id AS merchant_id, 
                        active_split_places::place_name AS place_name, split_posts::id AS post_id, split_posts::source AS source,
                        split_posts::month AS post_month, split_posts::kind AS kind;
places_posts_distinct = DISTINCT places_posts_distinct;

places_posts_counted = GROUP places_posts_distinct BY (merchant_id, place_name, post_month, kind);
places_posts_counted = FOREACH places_posts_counted GENERATE FLATTEN(group), COUNT(places_posts_distinct) AS kind_count;

-- flatten the groupings again
places_posts_flattened = FOREACH places_posts_counted GENERATE group::merchant_id AS merchant_id, group::place_name AS place_name, group::post_month AS post_month, 
                            group::kind AS kind, kind_count;

-- group again to place all sources and counts on same row
places_posts_regrouped = GROUP places_posts_flattened BY (merchant_id, place_name, post_month);

-- now use a UDF to format the outp
output_data = FOREACH places_posts_regrouped GENERATE FLATTEN(group), places_posts_flattened;

output_data = FOREACH output_data GENERATE group::merchant_id AS merchant_id, group::place_name AS place_name, group::post_month AS post_month, 
                            lm_udf.map_kind_counts(places_posts_flattened) AS counts,
                            lm_udf.sum_kind_counts(places_posts_flattened) AS total;

output_data = FILTER output_data BY total > 0;

STORE output_data INTO 'mongodb://$DB:$DB_PORT/localmeasure_metrics.kinds'
             USING com.mongodb.hadoop.pig.MongoInsertStorage('');

-- DUMP places_posts_counted;

