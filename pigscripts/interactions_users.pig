--Change these jar locations to point to the correct locations/version on your system.
REGISTER $JARFILES/mongo-java-driver-2.13.0.jar
REGISTER $JARFILES/mongo-hadoop-core-1.3.3-SNAPSHOT.jar
REGISTER $JARFILES/mongo-hadoop-pig-1.3.3-SNAPSHOT.jar
-- REGISTER /Users/tang/Projects/mongo-hadoop/examples/lm_posts/udf/datafu-1.2.0.jar
REGISTER '$LM_UDF/lm_udf.py' using org.apache.pig.scripting.jython.JythonScriptEngine as lm_udf;

posts = LOAD 'mongodb://$DB:$DB_PORT/localmeasure.posts' 
    USING com.mongodb.hadoop.pig.MongoLoader('id:chararray, post_time:chararray, secondary_venue_ids:chararray', 'id') 
    AS (id:chararray, post_time:chararray, secondary_venue_ids:chararray); 

places = LOAD 'mongodb://$DB:$DB_PORT/localmeasure.places' 
    USING com.mongodb.hadoop.pig.MongoLoader('id:chararray, name:chararray, merchant_id:chararray, venue_ids:chararray', 'id') 
    AS (id:chararray, name:chararray, merchant_id:chararray, venue_ids:chararray);

audits = LOAD 'mongodb://$DB:$DB_PORT/localmeasure.audits' 
    USING com.mongodb.hadoop.pig.MongoLoader('id:chararray, category:chararray, merchant_id:chararray, created_at:chararray, type:chararray, subject:map[], actor:map[]', 'id') 
    AS (id:chararray, category:chararray, merchant_id:chararray, created_at:chararray, type:chararray, subject, actor);

merchants = LOAD 'mongodb://$DB:$DB_PORT/localmeasure.merchants' 
    USING com.mongodb.hadoop.pig.MongoLoader('id, name, subscription', 'id');

merchants2 = FOREACH merchants GENERATE $0 AS id, $1 AS name, lm_udf.is_expired($2#'expires_at') AS expiry;
active_merchants = FILTER merchants2 BY expiry == 0;

-- Only work with places owned by NON-EXPIRED merchants
active_places = JOIN places BY merchant_id, active_merchants BY id;

-- Put venue_id's on a single row each ready to be joined with Posts
active_split_places = FOREACH active_places GENERATE places::name AS name, active_merchants::id AS merchant_id, 
                       FLATTEN(TOKENIZE(lm_udf.venue_id_strip(places::venue_ids))) AS venue_id;

-- Put venue_id's on a single row each ready to be joined with Places
split_posts = FOREACH posts GENERATE id, 
        CONCAT(SUBSTRING(post_time, 24, 28), SUBSTRING(post_time, 4, 7)) AS month,
        FLATTEN(TOKENIZE(lm_udf.venue_id_strip(secondary_venue_ids))) AS venue_id;

-- Work on only a month at a time
split_posts = FILTER split_posts BY month == '$MONTH';

places_posts_joined = JOIN active_split_places BY venue_id, split_posts BY venue_id;
places_posts_distinct = FOREACH places_posts_joined GENERATE active_split_places::name AS place_name, split_posts::id AS post_id,
                            active_split_places::merchant_id AS merchant_id;

places_posts_distinct = DISTINCT places_posts_distinct;

audits_filtered = FILTER audits BY (type MATCHES 'like' OR type MATCHES 'reply');
audits_filtered = FOREACH audits GENERATE type, created_at, actor#'label' AS user, subject#'origin_id' AS post_id;

audits_joined = JOIN audits_filtered BY post_id, places_posts_distinct BY post_id;

audits_monthly = FOREACH audits_joined GENERATE audits_filtered::type AS type, audits_filtered::user AS user, SUBSTRING(places_posts_distinct::post_id, 0, 2) AS source,
                CONCAT(SUBSTRING(audits_filtered::created_at, 24, 28), SUBSTRING(audits_filtered::created_at, 4, 7)) AS month,
                places_posts_distinct::place_name AS place_name, places_posts_distinct::merchant_id AS merchant_id;

audits_grouped = GROUP audits_monthly BY (merchant_id, place_name, month, user, source, type);

audits_grouped_counted = FOREACH audits_grouped GENERATE FLATTEN(group), COUNT(audits_monthly) AS audit_count_for_month;

audits_grouped_flattened = FOREACH audits_grouped_counted GENERATE group::merchant_id AS merchant_id, group::place_name AS place_name,
                            group::month AS month, group::user AS user, group::source AS source, group::type AS type, audit_count_for_month;

audits_flattened_regrouped = GROUP audits_grouped_flattened BY (merchant_id, place_name, month, user);

output_data = FOREACH audits_flattened_regrouped GENERATE FLATTEN(group), audits_grouped_flattened;

output_data = FOREACH output_data GENERATE group::merchant_id AS merchant_id, group::place_name AS place_name, group::month AS month, group::user AS user,
                lm_udf.map_interaction_counts(audits_grouped_flattened) AS counts,
                            lm_udf.sum_interaction_counts(audits_grouped_flattened) AS total;

STORE output_data INTO 'mongodb://$DB:$DB_PORT/localmeasure_metrics.interactions'
             USING com.mongodb.hadoop.pig.MongoInsertStorage('');
