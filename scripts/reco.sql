-- lets the windows accumulate more data
set 'commit.interval.ms'='2000';
set 'cache.max.bytes.buffering'='10000000';
set 'auto.offset.reset'='earliest';


-- 1. Raw json  of bid
DROP STREAM IF EXISTS json_bid_raw;
CREATE STREAM json_bid_raw (timestamp bigint, id varchar, advertiserId varchar, clickProbability double) with (kafka_topic='json-learning', value_format='json');


-- 2. Raw json  of win
DROP STREAM IF EXISTS json_win_raw;
CREATE STREAM json_win_raw (timestamp bigint, auctid  varchar, advertiserId varchar, winPrice double) with (kafka_topic='json-win', value_format='json');



-- bid with proper key
DROP STREAM IF EXISTS bid_proper_key;
CREATE STREAM bid_proper_key WITH (kafka_topic='bid_with_proper_key') AS SELECT id, clickProbability as cP FROM json_bid_raw PARTITION BY id;




-- 2. win of proper key
DROP STREAM IF EXISTS win_proper_key;
CREATE STREAM win_proper_key WITH (kafka_topic='win_with_proper_key') AS SELECT auctid, winPrice  FROM json_win_raw PARTITION BY auctid;





-- table bid with proper key
DROP TABLE IF EXISTS tbid;
DROP STREAM IF EXISTS tbid;
CREATE TABLE tbid (id varchar, cP double) WITH (kafka_topic='bid_with_proper_key', VALUE_FORMAT='JSON', key = 'id');



-- join tbid with json_win
DROP STREAM IF EXISTS reco;
CREATE STREAM reco AS SELECT w.auctid, w.winPrice, b.cP FROM WIN_PROPER_KEY  w LEFT JOIN tbid b ON w.auctid = b.id;


-- valid reco (not null)
DROP STREAM IF EXISTS reco_valid;
CREATE STREAM reco_valid AS SELECT * FROM reco WHERE cP >= 0;



