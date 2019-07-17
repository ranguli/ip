CREATE TABLE IF NOT EXISTS attack_log(
  src_ip TEXT, 
  isp_name TEXT, 
  event_timestamp TEXT,
  event_id TEXT,
  attempted_username TEXT,
  attempted_password TEXT,
  credential_signature TEXT,
  command TEXT,
  country_code TEXT, 
  country_code_alpha3 TEXT,
  country_name TEXT, 
  subdivision_code TEXT, 
  subdivision_name TEXT, 
  city_name TEXT, 
  postal_code TEXT, 
  continent_code TEXT, 
  continent_name TEXT, 
  latitude REAL, 
  longitude REAL, 
  time_zone TEXT, 
  accuracy_radius INT 
);

