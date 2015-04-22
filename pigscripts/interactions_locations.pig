--Change these jar locations to point to the correct locations/version on your system.
REGISTER $JARFILES/mongo-java-driver-2.13.0.jar
REGISTER $JARFILES/mongo-hadoop-core-1.3.3-SNAPSHOT.jar
REGISTER $JARFILES/mongo-hadoop-pig-1.3.3-SNAPSHOT.jar
-- REGISTER /Users/tang/Projects/mongo-hadoop/examples/lm_posts/udf/datafu-1.2.0.jar
REGISTER '$LM_UDF/lm_udf.py' using org.apache.pig.scripting.jython.JythonScriptEngine as lm_udf;

-- define TransposeTupleToBag datafu.pig.util.TransposeTupleToBag();

posts = LOAD 'mongodb://$DB:$DB_PORT/localmeasure.posts' 
    USING com.mongodb.hadoop.pig.MongoLoader('id:chararray, post_time:chararray, secondary_venue_ids:chararray', 'id') 
    AS (id:chararray, post_time:chararray, secondary_venue_ids:chararray); 

places = LOAD 'mongodb://$DB:$DB_PORT/localmeasure.places' 
    USING com.mongodb.hadoop.pig.MongoLoader('id:chararray, name:chararray, merchant_id:chararray, venue_ids:chararray', 'id') 
    AS (id:chararray, name:chararray, merchant_id:chararray, venue_ids:chararray);

audits = LOAD 'mongodb://$DB:$DB_PORT/localmeasure.audits' 
    USING com.mongodb.hadoop.pig.MongoLoader('id:chararray, category:chararray, merchant_id:chararray, created_at:chararray, type:chararray', 'id') 
    AS (id:chararray, category:chararray, merchant_id:chararray, created_at:chararray, type:chararray);

merchants = LOAD 'mongodb://$DB:$DB_PORT/localmeasure.merchants' 
    USING com.mongodb.hadoop.pig.MongoLoader('id, name, subscription', 'id');

merchants2 = FOREACH merchants GENERATE $0 AS id, $1 AS name, lm_udf.is_expired($2#'expires_at') AS expiry;
active_merchants = FILTER merchants2 BY expiry == 0;

active_places = JOIN places BY merchant_id, active_merchants BY id;

-- Strip off the leading [] chars in the venue_id array. This shouldn't be necessary with Mongo loader then flatten venue_ids to a row each.
active_split_places = FOREACH active_places GENERATE places::name AS name, places::merchant_id AS merchant_id, 
                        FLATTEN(TOKENIZE(SUBSTRING(places::venue_ids, 1, INDEXOF(places::venue_ids, ']', 0)))) AS venue_id;

-- Flatten teh posts collection similarly, TODO: create UDF's for all the date fields with a date_helper UDF
split_posts = FOREACH posts GENERATE id, 
        CONCAT(SUBSTRING(post_time, 24, 28), SUBSTRING(post_time, 4, 7)) AS month,
        CONCAT(SUBSTRING(post_time, 24, 28), CONCAT(SUBSTRING(post_time, 4, 7), SUBSTRING(post_time, 8, 10))) AS day,
        CONCAT(SUBSTRING(post_time, 24, 28), CONCAT(SUBSTRING(post_time, 4, 7), SUBSTRING(post_time, 8, 10), SUBSTRING(post_time, 11, 13))) AS hour,
        FLATTEN(TOKENIZE(SUBSTRING(secondary_venue_ids, 1, INDEXOF(secondary_venue_ids, ']', 0)))) AS venue_id;

places_posts_joined = JOIN active_split_places BY venue_id, split_posts BY venue_id;
places_posts_distinct = FOREACH places_posts_joined GENERATE active_split_places::merchant_id AS merchant_id, active_split_places::name AS place_name, split_posts::id AS post_id, split_posts::hour AS post_hour;
places_posts_distinct = DISTINCT places_posts_distinct;

places_posts_counted = GROUP places_posts_distinct BY (merchant_id, place_name, post_hour);
places_posts_counted = FOREACH places_posts_counted GENERATE group, (chararray)COUNT(places_posts_distinct) AS type_count;

audits_filtered = FILTER audits BY (category MATCHES 'interaction');
audits_joined = JOIN audits_filtered BY merchant_id, active_merchants BY id;

audits_hours = FOREACH audits_joined GENERATE audits_filtered::merchant_id AS merchant_id, audits_filtered::type AS type, 
                CONCAT(SUBSTRING(audits_filtered::created_at, 24, 28), CONCAT(SUBSTRING(audits_filtered::created_at, 4, 7), SUBSTRING(audits_filtered::created_at, 8, 10), SUBSTRING(audits_filtered::created_at, 11, 13))) AS hour;

audits_grouped = GROUP audits_hours BY (merchant_id, type, hour);
audits_grouped_counted = FOREACH audits_grouped GENERATE group, (chararray)COUNT(audits_hours) AS post_count_for_hour;
DUMP audits_grouped_counted;
