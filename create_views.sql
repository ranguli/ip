CREATE VIEW usernames AS SELECT username, count(username) as occurences FROM attack_log GROUP BY username ORDER BY occurences DESC;

CREATE VIEW passwords AS SELECT password, count(password) as occurences FROM attack_log GROUP BY password ORDER BY occurences DESC;
