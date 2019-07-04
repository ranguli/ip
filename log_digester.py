#!/usr/bin/env python
from datetime import datetime
import csv

class LogDigester:
    """ Simple class for processing log data into csv records """

    def auth_log_to_csv(self, logfile, outfile):
        """ Takes in an auth.log and outputs formatted csv """
        records = []
        
        with open(logfile) as f:
            for line in f:
                if "Disconnecting" in line:
                    line = line.split()
                    month = line[0]
                    day  = line[1].zfill(2)
                    hour, minute, second = line[2].split(":")
                    year = str(datetime.today().year)
                    unformatted_timestamp = [month, day, year, hour, minute, second]
                    timestamp = str(datetime.strptime(" ".join(unformatted_timestamp), '%b %d %Y %H %M %S'))
                    server = line[3]
                    user   = line[8]
                    source_ip = line[9]
                    records.append([source_ip, user, server, timestamp, hour])
            f.close()

        with open(outfile, "w+") as f:
            writer = csv.writer(f)
            # Write the header row
            writer.writerow(['source_ip','user','server','timestamp','date','time'])
            for record in records:
                writer.writerow(record)

logdigester = LogDigester()
logdigester.auth_log_to_csv("auth.log", "out.csv")
