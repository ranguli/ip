CREATE VIEW wordlist 
AS 
  SELECT DISTINCT attempted_username, attempted_password
FROM attack_log; 

CREATE VIEW countries
AS
  SELECT country_code_alpha3, country_name, count(source_ip) as attack_count 
  FROM attack_log 
  WHERE country_code_alpha3 IS NOT NULL
  GROUP BY country_code;

CREATE VIEW cities 
AS
  SELECT city_name, latitude, longitude, count(source_ip) as attack_count 
  FROM attack_log 
  WHERE city_name IS NOT NULL
  GROUP BY city_name;

CREATE VIEW attackers
AS
  SELECT DISTINCT source_ip, country_name, subdivision_name, city_name, latitude, longitude, count(source_ip) as attack_count 
  FROM attack_log 
  GROUP BY source_ip 
  ORDER BY attack_count DESC;


