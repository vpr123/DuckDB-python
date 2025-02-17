CREATE TEMPORARY TABLE t_stg_event_continuous_ranges AS
SELECT kafka_partition,
       first_offset,
       MAX(kafka_offset) last_offset
  FROM (SELECT kafka_partition,
               kafka_offset,
               MAX(CASE
                    WHEN (kafka_offset - offset_prev) = 1 THEN NULL
                    ELSE kafka_offset
                   END) OVER (PARTITION BY kafka_partition ORDER BY kafka_offset ROWS BETWEEN unbounded preceding and current row) first_offset
          FROM (SELECT kafka_partition,
                       kafka_offset,
                       lag (kafka_offset) OVER (PARTITION BY kafka_partition ORDER BY kafka_offset) offset_prev
                  FROM t_stg_events) s ) t
 GROUP BY t.kafka_partition,
          t.first_offset