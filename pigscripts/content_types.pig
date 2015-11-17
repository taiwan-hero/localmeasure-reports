REGISTER jar/mongo-java-driver-2.13.0.jar
REGISTER jar/mongo-hadoop-core-1.3.3-SNAPSHOT.jar
REGISTER jar/mongo-hadoop-pig-1.3.3-SNAPSHOT.jar
REGISTER udf/lm_udf.py using org.apache.pig.scripting.jython.JythonScriptEngine as lm_udf;

posts = LOAD 'mongodb://$DB/localmeasure.posts' 
    USING com.mongodb.hadoop.pig.MongoLoader('id:chararray, post_time:chararray, secondary_venue_ids:chararray, kind:chararray, poster_id:chararray', 'id') 
    AS (id:chararray, post_time:chararray, secondary_venue_ids:chararray, kind:chararray, poster_id:chararray); 

places = LOAD 'mongodb://$DB/localmeasure.places' 
    USING com.mongodb.hadoop.pig.MongoLoader('id:chararray, name:chararray, merchant_id:chararray, venue_ids:chararray', 'id') 
    AS (id:chararray, name:chararray, merchant_id:chararray, venue_ids:chararray);

merchants = LOAD 'mongodb://$DB/localmeasure.merchants' 
    USING com.mongodb.hadoop.pig.MongoLoader('id, name, subscription, linked_accounts', 'id');

merchants2 =            FOREACH merchants GENERATE $0 AS id,
                                                   $1 AS name,
                                                   lm_udf.is_expired($2#'expires_at') AS expiry,
                                                   lm_udf.parse_linked_accounts($3) AS linked_accounts;

active_merchants =      FILTER merchants2 BY expiry == 0;

active_places =         JOIN places BY merchant_id, active_merchants BY id;

-- Strip off the leading [] chars in the venue_id array. This shouldn't be necessary with Mongo loader then flatten venue_ids to a row each.
active_split_places =   FOREACH active_places GENERATE places::name AS place_name, 
                                                       active_merchants::id AS merchant_id,
                                                       FLATTEN(TOKENIZE(lm_udf.venue_id_strip(venue_ids))) AS venue_id,
                                                       active_merchants::linked_accounts AS linked_accounts;

-- Flatten teh posts collection similarly, TODO: create UDF's for all the date fields with a date_helper UDF
split_posts =           FOREACH posts GENERATE id,
                                               poster_id,
                                               kind, 
                                               SUBSTRING(id, 0, 2) AS source,
                                               lm_udf.get_month(post_time) AS month,
                                               FLATTEN(TOKENIZE(lm_udf.venue_id_strip(secondary_venue_ids))) AS venue_id;

split_posts =           FILTER split_posts BY month == '$MONTH';

places_posts_joined =   JOIN active_split_places BY venue_id, split_posts BY venue_id;

places_posts_distinct = FOREACH places_posts_joined GENERATE active_split_places::merchant_id AS merchant_id,
                                                             active_split_places::place_name AS place_name,
                                                             split_posts::id AS post_id,
                                                             split_posts::month AS post_month,
                                                             split_posts::source AS source,
                                                             split_posts::kind AS kind,
                                                             lm_udf.is_own_post(active_split_places::linked_accounts, split_posts::poster_id) AS own_post;

places_posts_distinct = FILTER places_posts_distinct BY own_post == 0;

places_posts_distinct = DISTINCT places_posts_distinct;

places_posts_counted =  GROUP places_posts_distinct BY (merchant_id, place_name, post_month, source, kind);
places_posts_flattened =  FOREACH places_posts_counted GENERATE group.merchant_id AS merchant_id,
                                                               group.place_name AS place_name,
                                                               group.post_month AS post_month,
                                                               group.source AS source,
                                                               group.kind AS kind,
                                                              COUNT(places_posts_distinct) AS kind_count;

-- group again to place all sources and counts on same row
places_posts_regrouped = GROUP places_posts_flattened BY (merchant_id, place_name, post_month);

-- first flatten the group again, in preparation for insertion into Mongo
output_data =           FOREACH places_posts_regrouped GENERATE group.merchant_id AS merchant_id,
                                                                group.place_name AS place_name,
                                                                group.post_month AS post_month,
                                                                lm_udf.map_kind_counts(places_posts_flattened) AS counts,
                                                                lm_udf.sum_kind_counts(places_posts_flattened) AS total;

output_data =           FILTER output_data BY total > 0;

STORE output_data INTO 'mongodb://$DB/localmeasure_metrics.content'
             USING com.mongodb.hadoop.pig.MongoInsertStorage('');


