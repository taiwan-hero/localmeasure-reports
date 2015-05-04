--Change these jar locations to point to the correct locations/version on your system.
REGISTER $JARFILES/mongo-java-driver-2.13.0.jar
REGISTER $JARFILES/mongo-hadoop-core-1.3.3-SNAPSHOT.jar
REGISTER $JARFILES/mongo-hadoop-pig-1.3.3-SNAPSHOT.jar

REGISTER '$LM_UDF/lm_udf.py' using org.apache.pig.scripting.jython.JythonScriptEngine as lm_udf;

posts = LOAD 'mongodb://$DB:$DB_PORT/localmeasure.posts' 
        USING com.mongodb.hadoop.pig.MongoLoader('id:chararray, post_time:chararray', 'id') 
        AS (id:chararray, post_time:chararray);

audits = LOAD 'mongodb://$DB:$DB_PORT/localmeasure.audits' 
          USING com.mongodb.hadoop.pig.MongoLoader('merchant_id:chararray, created_at:chararray, type:chararray, subject:map[], actor:map[]', '') 
          AS (merchant_id:chararray, created_at:chararray, type:chararray, subject, actor);

posts_timed =       FOREACH posts GENERATE id, post_time, lm_udf.get_month(post_time) AS month;

audits_timed =      FOREACH audits GENERATE type,
                                            created_at, 
                                            lm_udf.get_month(created_at) AS month,
                                            actor#'label' AS user, 
                                            subject#'origin_id' AS post_id,
                                            merchant_id;

audits_timed =          DISTINCT audits_timed;

audits_filtered =       FILTER audits_timed BY (type MATCHES 'like' OR type MATCHES 'reply' OR type MATCHES 'tag' OR type MATCHES 'follow') AND (month == '$MONTH');

audits_posts_joined =   JOIN posts_timed BY id, audits_filtered BY post_id;

interactions =          FOREACH audits_posts_joined GENERATE posts_timed::id AS post_id,
                                                        lm_udf.time_diff(posts_timed::post_time, audits_filtered::created_at) AS response_time,
                                                        audits_filtered::month AS month,
                                                        audits_filtered::user AS user,
                                                        audits_filtered::merchant_id AS merchant_id;

post_interactions =     GROUP interactions BY (merchant_id, user, month, post_id);

min_time_interactions = FOREACH post_interactions GENERATE group.merchant_id AS merchant_id,
                                                            group.user AS user,
                                                            group.month AS month,
                                                            group.post_id AS post_id,
                                                            lm_udf.get_min_interaction_time(interactions) AS min_response_time;

response_time_by_user = GROUP min_time_interactions BY (merchant_id, month, user);

averages =              FOREACH response_time_by_user GENERATE group.merchant_id AS merchant_id, 
                                                                group.month AS month,
                                                                group.user AS user, 
                                                                AVG(min_time_interactions.min_response_time) AS average;

STORE averages           INTO 'mongodb://$DB:$DB_PORT/localmeasure_metrics.response_times'
                           USING com.mongodb.hadoop.pig.MongoInsertStorage('');
