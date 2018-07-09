-- lets the windows accumulate more data
set 'commit.interval.ms'='2000';
set 'cache.max.bytes.buffering'='10000000';
set 'auto.offset.reset'='earliest';


-- 1. Raw json  of tweets
DROP STREAM IF EXISTS tweets_raw;
CREATE STREAM tweets_raw (timestamp_ms bigint, id bigint, text varchar, favorite_count int, user varchar, entities  varchar, lang varchar) with (kafka_topic='tweets', value_format='json');




-- 2. Transform infos, for example: Nested user => user.xxx, nested entities => entities.xxx
DROP STREAM IF EXISTS tweets_transformed;
CREATE STREAM tweets_transformed AS SELECT TIMESTAMPTOSTRING(timestamp_ms, 'yyyy-MM-dd') as created_at,  id, text, favorite_count as fav, EXTRACTJSONFIELD(user, '$.id') AS user_id, EXTRACTJSONFIELD(user, '$.name') AS user_name, EXTRACTJSONFIELD(user, '$.screen_name') AS user_screen_name, EXTRACTJSONFIELD(entities, '$.hashtags') AS hashtags, EXTRACTJSONFIELD(entities, '$.user_mentions') AS user_mentions, lang FROM tweets_raw;


-- 3. TABLE: userid - count
DROP TABLE IF EXISTS userid_count;
CREATE TABLE userid_count AS SELECT user_id as userid, COUNT(*) as count FROM tweets_transformed GROUP BY user_id;


-- 4. TABLE: userid - fav
DROP TABLE IF EXISTS userid_fav;
CREATE TABLE userid_fav AS SELECT user_id as userid, SUM(fav) as fav FROM tweets_transformed GROUP BY user_id;


-- 5. TABLE: userid - screen.name
DROP TABLE IF EXISTS userid_name;
CREATE TABLE userid_name AS SELECT user_id as userid, TOPK(user_screen_name, 1) as name FROM tweets_transformed GROUP BY user_id;


-- 6. STREAM: tweets-mario-fr
DROP STREAM IF EXISTS tweets_mario_fr;
CREATE STREAM tweets_mario_fr AS SELECT id, user_id, user_screen_name as screen_name, created_at, hashtags, text  FROM tweets_transformed WHERE text LIKE '%mario%' AND lang='fr';



