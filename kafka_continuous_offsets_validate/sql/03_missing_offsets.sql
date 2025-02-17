WITH q AS
(
 SELECT t.*,
        row_number() over (partition by kafka_partition order by first_offset) as rnk
   FROM t_stg_event_continuous_ranges t
)
SELECT t1.kafka_partition,
       t1.last_offset + 1  AS first_missing_offset,
       t2.first_offset - 1 AS last_missing_offset
  FROM q t1
  JOIN q t2
    ON t1.kafka_partition = t2.kafka_partition
   AND t1.rnk = t2.rnk - 1