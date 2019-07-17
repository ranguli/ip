CREATE VIEW wordlist 
AS 
  SELECT DISTINCT attempted_username, attempted_password
FROM attack_log; 

CREATE VIEW countries
AS
  SELECT country_code_alpha3, country_name, count(src_ip) as attack_count 
  FROM attack_log 
  WHERE country_code_alpha3 IS NOT NULL AND (event_id='cowrie.login.success' OR event_id='cowrie.login.failed')
  GROUP BY country_code;

CREATE VIEW cities 
AS
  SELECT city_name, latitude, longitude, count(src_ip) as attack_count 
  FROM attack_log 
  WHERE city_name IS NOT NULL AND (event_id='cowrie.login.success' OR event_id='cowrie.login.failed')
  GROUP BY city_name;

CREATE VIEW attackers
AS
  SELECT DISTINCT src_ip, isp_name, continent_code, country_name, subdivision_name, city_name, accuracy_radius, latitude, longitude, count(src_ip) as attack_count 
  FROM attack_log 
  GROUP BY src_ip 
  ORDER BY attack_count DESC;


