REGISTER jar/mongo-java-driver-2.13.0.jar
REGISTER jar/mongo-hadoop-core-1.3.3-SNAPSHOT.jar
REGISTER jar/mongo-hadoop-pig-1.3.3-SNAPSHOT.jar
REGISTER udf/lm_udf.py using org.apache.pig.scripting.jython.JythonScriptEngine as lm_udf;

posts = LOAD 'mongodb://$DB/localmeasure.posts' 
        USING com.mongodb.hadoop.pig.MongoLoader('id:chararray, post_time:chararray, kind:chararray, secondary_venue_ids:chararray, rating', 'id') 
        AS (id:chararray, post_time:chararray, kind:chararray, secondary_venue_ids:chararray, rating); 

places = LOAD 'mongodb://$DB/localmeasure.places' 
         USING com.mongodb.hadoop.pig.MongoLoader('id:chararray, name:chararray, merchant_id:chararray, venue_ids:chararray', 'id') 
         AS (id:chararray, name:chararray, merchant_id:chararray, venue_ids:chararray);

merchants = LOAD 'mongodb://$DB/localmeasure.merchants' 
            USING com.mongodb.hadoop.pig.MongoLoader('id, name, subscription', 'id');

merchants2 =                FOREACH merchants GENERATE $0 AS id, $1 AS name, lm_udf.is_expired($2#'expires_at') AS expiry;
active_merchants =          FILTER merchants2 BY expiry == 0;

-- Only work with places owned by NON-EXPIRED merchants
active_places =             JOIN places BY merchant_id, active_merchants BY id;

-- Put venue_id's on a single row each ready to be joined with Posts
active_split_places =       FOREACH active_places GENERATE places::name AS name, 
                                                    active_merchants::id AS merchant_id, 
                                                    FLATTEN(TOKENIZE(lm_udf.venue_id_strip(places::venue_ids))) AS venue_id;

split_posts =               FILTER posts BY kind MATCHES 'review';

-- Put venue_id's on a single row each ready to be joined with Places
split_posts =               FOREACH split_posts GENERATE id, 
                                        SUBSTRING(id, 0, 2) AS source,
                                        lm_udf.get_month(post_time) AS month,
                                        FLATTEN(TOKENIZE(lm_udf.venue_id_strip(secondary_venue_ids))) AS venue_id,
                                        rating#'value' AS value,
                                        rating#'scale' AS scale;

-- Work on only a month at a time
split_posts =               FILTER split_posts BY (month == '$MONTH' AND source == 'FB');

places_posts_joined =       JOIN active_split_places BY venue_id, split_posts BY venue_id;

places_posts_distinct =     FOREACH places_posts_joined GENERATE active_split_places::merchant_id AS merchant_id,
                                                             active_split_places::name AS place_name, 
                                                             split_posts::month AS month,
                                                             split_posts::source AS source,
                                                             split_posts::id AS post_id,
                                                             (chararray)split_posts::value AS value;

places_posts_distinct =      DISTINCT places_posts_distinct;

reviews_grouped =            GROUP places_posts_distinct BY (merchant_id, place_name, month, source, value);

reviews_grouped_flattened =    FOREACH reviews_grouped GENERATE group.merchant_id AS merchant_id, 
                                                            group.place_name AS place_name,
                                                            group.month AS month, 
                                                            group.source AS source,
                                                            group.value AS value,
                                                            COUNT(places_posts_distinct) AS review_count_for_month;

reviews_flattened_regrouped = GROUP reviews_grouped_flattened BY (merchant_id, place_name, month);

output_data =               FOREACH reviews_flattened_regrouped GENERATE group.merchant_id AS merchant_id, 
                                                                        group.place_name AS place_name, 
                                                                        group.month AS month, 
                                                                        lm_udf.map_review_counts(reviews_grouped_flattened) AS counts;

STORE output_data           INTO 'mongodb://$DB/localmeasure_metrics.reviews'
                            USING com.mongodb.hadoop.pig.MongoInsertStorage('');

