#!/usr/bin/env python3
import random
from random import randrange
from random_word import RandomWords
from multiprocessing import Process
import datetime
from faker import Faker
import json
import redis
import sys
import uuid
import logging
from utils import read_config, get_db_connection, setup_logging

# redishost=<redis host>
# redisport=6379
# R = redis.Redis(host=redishost, port=redisport, decode_responses=True)


US_STATES = [
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
]
GENDER = ["M", "F"]
INSURANCE = ["ALTADENA", "BLUE SHIELD", "KAISER", "ANTHEM", "BLUE CROSS"]
ADTS = ["08", "01", "02", "04", "32", "64"]


def insert_data(psql_cur, table, sample):
    query = "INSERT INTO {} (patientjson) VALUES (%s);".format(table)
    psql_cur.execute(query, (json.dumps(sample),))
    psql_cur.connection.commit()


def main():
    # Setup logging
    setup_logging()
    logging.info("Starting data generation process")

    # Read configuration
    config = read_config("etl.config")

    # Get database connection
    try:
        psql_conn, psql_cur = get_db_connection(config)
        logging.info("Successfully connected to PostgreSQL database")
    except Exception as e:
        logging.error(f"Failed to connect to database: {e}")
        sys.exit(1)

    # postgresql table
    table_name = config.get("reportdb", "table", fallback="patient_data")
    num_files = 2000000

    for i in range(num_files):
        pid = str(randrange(1000000, 9999999))
        filename = "pid_" + pid + "_" + uuid.uuid4().hex + ".json"

        fake = Faker()
        updatedat = fake.date_time_between(start_date="-2y", end_date="now").strftime(
            "%Y-%m-%d %H:%M"
        )
        dob = fake.date_time_between(start_date="-40y", end_date="-10y").strftime(
            "%Y-%m-%d"
        )
        dobint = dob.replace("-", "")
        name = fake.name().split(" ")
        adt = random.choice(ADTS)

        r = RandomWords()
        tenantid = r.get_random_word()
        ins_name = r.get_random_word().upper()

        visitnum = str(randrange(10000000, 99999999))
        somecode = "S" + str(randrange(10000, 99999))

        sample = json.dumps(
            {
                "patientId": pid,
                "tenantId": tenantid,
                "dob": dob,
                "id": pid,
                "updatedAt": updatedat,
                "createdAt": updatedat,
                "visitNumber": visitnum,
                "MSH": "MSH|^~&|EPICCARE|WB^WBPC|||20230110144357|"
                + somecode
                + "|ADT^"
                + adt
                + "^ADT_A01|400815517|P|2.3",
                "EVN": "EVN|"
                + adt
                + "|20230110144357||REGCHECKCOMP_"
                + adt
                + "|"
                + somecode
                + "^"
                + name[-1].upper()
                + "^"
                + name[0].upper()
                + "^ANAME^^^^^WB^^^^^WBPC||WBPC^1740348929^SOMENAME",
                "PID": "PID|1||14891584^^^^EPI~62986117^^^^SOMERN||"
                + name[0].upper()
                + "^"
                + name[-1].upper()
                + "||"
                + dobint
                + "|"
                + random.choice(GENDER)
                + "|||"
                + fake.street_address().upper()
                + "^^"
                + fake.city().upper()
                + "^"
                + random.choice(US_STATES)
                + "^"
                + fake.postcode().upper()
                + "^USA^P^^SC",
                "PV1": "PV1||O|168 ~219~C~PMA^^^^^^^^^||||277^"
                + name[-1].upper()
                + "^BONNIE^^^^|||||||||| ||2688684|||||||||||||||||||||||||202211031408||||||002376853",
                "IN1": "IN1|1|PRE2||"
                + random.choice(INSURANCE)
                + "|PO BOX 23523^WELLINGTON^ON^98111|||19601||||||||"
                + name[-1].upper()
                + "^"
                + name[0].upper()
                + "^M|F|||||||||||||||||||ZKA"
                + visitnum
                + "",
            }
        )

        # local files
        # with open('sample_jsons/'+filename,"w") as json_file:
        #    #json_file.write(json.dumps(sample))
        #    json.dump(json.loads(json.dumps(sample)), json_file)

        # RedisJSON
        # R.json().set(filename, '$', json.dumps(sample))

        # postgresql JSONB
        try:
            insert_data(psql_cur, table, sample)
        except Exception as e:
            logging.error(f"Error inserting data for {filename}: {e}")
            # Continue with next record instead of failing completely
            continue

        # Log progress every 10000 records
        if i % 10000 == 0:
            logging.info(f"Generated {i} records")

    logging.info(f"Completed data generation. Total records: {num_files}")


if __name__ == "__main__":
    p1 = Process(target=main)
    p2 = Process(target=main)
    p3 = Process(target=main)
    p4 = Process(target=main)
    p5 = Process(target=main)
    p6 = Process(target=main)

    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p5.start()
    p6.start()

    p1.join()
    p2.join()
    p3.join()
    p4.join()
    p5.join()
    p6.join()
