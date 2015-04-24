--Change these jar locations to point to the correct locations/version on your system.
REGISTER $JARFILES/mongo-java-driver-2.13.0.jar
REGISTER $JARFILES/mongo-hadoop-core-1.3.3-SNAPSHOT.jar
REGISTER $JARFILES/mongo-hadoop-pig-1.3.3-SNAPSHOT.jar

REGISTER '$LM_UDF/lm_udf.py' using org.apache.pig.scripting.jython.JythonScriptEngine as lm_udf;

posts = LOAD 'mongodb://$DB:$DB_PORT/localmeasure.posts' 
    USING com.mongodb.hadoop.pig.MongoLoader('id:chararray, post_time:chararray, secondary_venue_ids:chararray, text:chararray', 'id') 
    AS (id:chararray, post_time:chararray, secondary_venue_ids:chararray, text:chararray); 

places = LOAD 'mongodb://$DB:$DB_PORT/localmeasure.places' 
    USING com.mongodb.hadoop.pig.MongoLoader('id:chararray, name:chararray, merchant_id:chararray, venue_ids:chararray', 'id')
    AS (id:chararray, name:chararray, merchant_id:chararray, venue_ids:chararray);

merchants = LOAD 'mongodb://$DB:$DB_PORT/localmeasure.merchants'
    USING com.mongodb.hadoop.pig.MongoLoader('id, name, subscription', 'id');

-- Filter off Expired merchants, joining places to get active places
merchants2 = FOREACH merchants GENERATE $0 AS id, $1 AS name, lm_udf.is_expired($2#'expires_at') AS expiry;
active_merchants = FILTER merchants2 BY expiry == 0;
active_places = JOIN places BY merchant_id, active_merchants BY id;

-- Strip off the leading [] chars in the venue_id array. This shouldn't be necessary with Mongo loader then flatten venue_ids to a row each.
active_split_places = FOREACH active_places GENERATE active_merchants::id AS merchant_id, places::name AS place_name, 
                        FLATTEN(TOKENIZE(lm_udf.venue_id_strip(venue_ids))) AS venue_id;

-- Flatten teh posts collection similarly, TODO: create UDF's for all the date fields with a date_helper UDF
split_posts = FOREACH posts GENERATE id, text, 
        SUBSTRING(id, 0, 2) AS source,
        CONCAT(SUBSTRING(post_time, 24, 28), SUBSTRING(post_time, 4, 7)) AS month,
        FLATTEN(TOKENIZE(lm_udf.venue_id_strip(secondary_venue_ids))) AS venue_id;

split_posts = FILTER split_posts BY month == '$MONTH';

places_posts_joined = JOIN active_split_places BY venue_id, split_posts BY venue_id;
places_posts_distinct = FOREACH places_posts_joined GENERATE merchant_id::merchant_id AS merchant_id, active_split_places::place_name AS place_name, split_posts::id AS post_id, 
                        split_posts::month AS post_month, split_posts::text AS text, split_posts::source as source;

places_posts_distinct = DISTINCT places_posts_distinct;

split_text_flat = FOREACH places_posts_distinct GENERATE merchant_id, place_name, post_month, source,
        FLATTEN(TOKENIZE(lm_udf.text_strip(text))) AS word;

split_text_flat = FILTER split_text_flat BY word != '';

-- group to generate the counts
places_posts_counted = GROUP split_text_flat BY (merchant_id, place_name, post_month, source, word);
places_posts_counted = FOREACH places_posts_counted GENERATE FLATTEN(group), COUNT(split_text_flat) AS word_count;

-- flatten the groupings again
places_posts_flattened = FOREACH places_posts_counted GENERATE group::merchant_id AS merchant_id, group::place_name AS place_name, group::post_month AS post_month, 
                            group::word AS word, group::source AS source, word_count;

-- group again to place all sources and counts on same row
places_posts_regrouped = GROUP places_posts_flattened BY (merchant_id, place_name, post_month, word);

-- now use a UDF to format the outp
output_data = FOREACH places_posts_regrouped GENERATE FLATTEN(group), places_posts_flattened;

output_data = FOREACH output_data GENERATE group::merchant_id AS merchant_id, group::place_name AS place_name, group::post_month AS post_month, 
                            group::word AS word, lm_udf.map_keyword_source_counts(places_posts_flattened) AS counts,
                            lm_udf.sum_source_counts(places_posts_flattened) AS total;

output_data = FILTER output_data BY total > 1;

STORE output_data INTO 'mongodb://$DB:$DB_PORT/localmeasure_metrics.keywords'
             USING com.mongodb.hadoop.pig.MongoInsertStorage('');


