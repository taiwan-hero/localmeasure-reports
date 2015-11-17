REGISTER jar/mongo-java-driver-2.13.0.jar
REGISTER jar/mongo-hadoop-core-1.3.3-SNAPSHOT.jar
REGISTER jar/mongo-hadoop-pig-1.3.3-SNAPSHOT.jar
REGISTER udf/lm_udf.py using org.apache.pig.scripting.jython.JythonScriptEngine as lm_udf;

posts = LOAD 'mongodb://$DB/localmeasure.posts' 
        USING com.mongodb.hadoop.pig.MongoLoader('id:chararray, post_time:chararray, secondary_venue_ids:chararray, poster_id:chararray', 'id') 
        AS (id:chararray, post_time:chararray, secondary_venue_ids:chararray, poster_id:chararray); 

places = LOAD 'mongodb://$DB/localmeasure.places' 
         USING com.mongodb.hadoop.pig.MongoLoader('id:chararray, name:chararray, merchant_id:chararray, venue_ids:chararray', 'id') 
         AS (id:chararray, name:chararray, merchant_id:chararray, venue_ids:chararray);

audits = LOAD 'mongodb://$DB/localmeasure.audits' 
        USING com.mongodb.hadoop.pig.MongoLoader('id:chararray, category:chararray, merchant_id:chararray, created_at:chararray, type:chararray, subject:map[], actor:map[]', 'id') 
        AS (id:chararray, category:chararray, merchant_id:chararray, created_at:chararray, type:chararray, subject, actor);

merchants = LOAD 'mongodb://$DB/localmeasure.merchants' 
            USING com.mongodb.hadoop.pig.MongoLoader('id, name, subscription, linked_accounts', 'id');

merchants2 =            FOREACH merchants GENERATE $0 AS id,
                                                   $1 AS name,
                                                   lm_udf.is_expired($2#'expires_at') AS expiry,
                                                   lm_udf.parse_linked_accounts($3) AS linked_accounts;

active_merchants =  FILTER merchants2 BY expiry == 0;

-- Only work with places owned by NON-EXPIRED merchants
active_places =     JOIN places BY merchant_id, active_merchants BY id;

-- Put venue_id's on a single row each ready to be joined with Posts
active_split_places = FOREACH active_places GENERATE places::name AS name, 
                                                    active_merchants::id AS merchant_id, 
                                                    FLATTEN(TOKENIZE(lm_udf.venue_id_strip(places::venue_ids))) AS venue_id,
                                                    active_merchants::linked_accounts AS linked_accounts;

-- Put venue_id's on a single row each ready to be joined with Places
split_posts =       FOREACH posts GENERATE id,
                                        poster_id,
                                        lm_udf.get_month(post_time) AS month,
                                        FLATTEN(TOKENIZE(lm_udf.venue_id_strip(secondary_venue_ids))) AS venue_id;

-- Work on only a month at a time
split_posts =       FILTER split_posts BY month == '$MONTH';

places_posts_joined =   JOIN active_split_places BY venue_id, split_posts BY venue_id;

places_posts_distinct = FOREACH places_posts_joined GENERATE active_split_places::name AS place_name, 
                                                             split_posts::id AS post_id,
                                                             active_split_places::merchant_id AS merchant_id,
                                                             lm_udf.is_own_post(active_split_places::linked_accounts, split_posts::poster_id) AS own_post;

places_posts_distinct = FILTER places_posts_distinct BY own_post == 0;
places_posts_distinct = DISTINCT places_posts_distinct;

audits_filtered =   FILTER audits BY (type MATCHES 'like' OR type MATCHES 'reply' OR type MATCHES 'tag' OR type MATCHES 'follow');

audits_filtered =   FOREACH audits_filtered GENERATE type, 
                                                     merchant_id,
                                                     CONCAT(SUBSTRING(created_at, 24, 28), SUBSTRING(created_at, 4, 7)) AS month,
                                                     actor#'label' AS user, 
                                                     subject#'origin_id' AS post_id;

audits_filtered =   FOREACH audits_filtered GENERATE type, 
                                                     merchant_id,
                                                     month,
                                                     user, 
                                                     post_id,
                                                     SUBSTRING(post_id, 0, 2) AS source;

audits_filtered =   FILTER audits_filtered BY month == '$MONTH';
audits_filtered =   DISTINCT audits_filtered;

follows =           FILTER audits_filtered BY (type MATCHES 'follow');
interactions =      FILTER audits_filtered BY (type MATCHES 'like' OR type MATCHES 'reply' OR type MATCHES 'tag');

-- here, we create a new type called 'int' for unique posts
unique =            FILTER interactions BY (type MATCHES 'like' OR type MATCHES 'reply');
unique =            FOREACH unique GENERATE 'int' AS type,
                                            merchant_id,
                                            month,
                                            user,
                                            post_id,
                                            source;
unique =            DISTINCT unique;

-- tack unique 'int' table onto usual interactions table
int_all =           UNION interactions, unique;

audits_joined =     JOIN int_all BY post_id, places_posts_distinct BY post_id;

-- filter out the results where merchant_id's dont match
audits_joined =     FILTER audits_joined BY places_posts_distinct::merchant_id == int_all::merchant_id;

audits_monthly =    FOREACH audits_joined GENERATE int_all::type AS type, 
                                                   int_all::user AS user, 
                                                   int_all::source AS source, 
                                                   int_all::month AS month,
                                                   places_posts_distinct::place_name AS place_name, 
                                                   places_posts_distinct::merchant_id AS merchant_id;

-- HACKTASTIC: join follows to places via USER handles. First, get places and users
places_users =              FOREACH audits_monthly GENERATE user, place_name, merchant_id;
places_users =              DISTINCT places_users;

-- Next, join places and users 
follows_places_joined =     JOIN follows BY user, places_users BY user;

-- filter out the results where merchant_id's dont match
follows_places_joined =     FILTER follows_places_joined BY places_users::merchant_id == follows::merchant_id;

follows_places_users =      FOREACH follows_places_joined GENERATE follows::type AS type, 
                                                              follows::user AS user,
                                                              follows::source AS source, 
                                                              follows::month AS month, 
                                                              places_users::place_name AS place_name, 
                                                              places_users::merchant_id AS merchant_id;

interactions_all =          UNION audits_monthly, follows_places_users;

audits_grouped =            GROUP interactions_all BY (merchant_id, place_name, month, user, source, type);

audits_grouped_flattened =    FOREACH audits_grouped GENERATE group.merchant_id AS merchant_id, 
                                                            group.place_name AS place_name,
                                                            group.month AS month, 
                                                            group.user AS user, 
                                                            group.source AS source, 
                                                            group.type AS type, 
                                                            COUNT(interactions_all) AS audit_count_for_month;

audits_flattened_regrouped = GROUP audits_grouped_flattened BY (merchant_id, place_name, month, user);

output_data =               FOREACH audits_flattened_regrouped GENERATE group.merchant_id AS merchant_id, 
                                                                        group.place_name AS place_name, 
                                                                        group.month AS month, 
                                                                        group.user AS user,
                                                                        lm_udf.map_interaction_counts(audits_grouped_flattened) AS counts;

STORE output_data           INTO 'mongodb://$DB/localmeasure_metrics.interactions'
                            USING com.mongodb.hadoop.pig.MongoInsertStorage('');

