CREATE VIEW dates 
AS
  SELECT event_timestamp, count(event_timestamp) as attack_count 
  FROM attack_log 
  GROUP BY event_timestamp;

