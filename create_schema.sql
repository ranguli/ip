CREATE TABLE IF NOT EXISTS attack_log(
  source_ip TEXT, 
  event_timestamp TEXT,
  country_code TEXT, 
  country_code_alpha3 TEXT,
  country_name TEXT, 
  subdivision_code TEXT, 
  subdivision_name TEXT, 
  city_name TEXT, 
  latitude REAL, 
  longitude REAL
);

