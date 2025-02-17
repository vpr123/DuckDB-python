CREATE TEMPORARY TABLE t_stg_events AS
WITH events AS
 (
 SELECT DISTINCT
        CAST(kafka_partition AS INTEGER) kafka_partition,
        CAST(kafka_offset AS INTEGER) kafka_offset,
        CAST(event_ts AS DATE) as event_dt
   FROM kafka_messages
)
SELECT kafka_partition,
       kafka_offset
  FROM events
 WHERE event_dt >= '$$start_date$$'
   AND kafka_offset >= $$start_offset$$

