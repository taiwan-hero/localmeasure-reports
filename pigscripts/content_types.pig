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
active_split_places = FOREACH active_places GENERATE places::name AS name, places::merchant_id AS merchant_id, 
                        FLATTEN(TOKENIZE(SUBSTRING(places::venue_ids, 1, INDEXOF(places::venue_ids, ']', 0)))) AS venue_id;

-- Flatten teh posts collection similarly, TODO: create UDF's for all the date fields with a date_helper UDF
split_posts = FOREACH posts GENERATE id, kind, 
        CONCAT(SUBSTRING(post_time, 24, 28), SUBSTRING(post_time, 4, 7)) AS month,
        FLATTEN(TOKENIZE(SUBSTRING(secondary_venue_ids, 1, INDEXOF(secondary_venue_ids, ']', 0)))) AS venue_id;

places_posts_joined = JOIN active_split_places BY venue_id, split_posts BY venue_id;
places_posts_distinct = FOREACH places_posts_joined GENERATE active_split_places::merchant_id AS merchant_id, 
                        active_split_places::name AS place_name, split_posts::id AS post_id, 
                        split_posts::month AS post_month, split_posts::kind AS kind;
places_posts_distinct = DISTINCT places_posts_distinct;

places_posts_counted = GROUP places_posts_distinct BY (merchant_id, place_name, post_month, kind);
places_posts_counted = FOREACH places_posts_counted GENERATE group, COUNT(places_posts_distinct) AS kind_count;

DUMP places_posts_counted;

