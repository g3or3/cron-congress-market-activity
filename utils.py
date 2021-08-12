from transaction import extractData
from dotenv import load_dotenv
from datetime import datetime
from zipfile import ZipFile
from difflib import Differ
import urllib.request
import pandas as pd
import sqlalchemy
import shutil
import redis
import sys
import os
import io


def connectDb():
    load_dotenv()
    try:
        db_url = os.getenv("DEV_DATABASE")
        return sqlalchemy.create_engine(db_url)
    except sys.exc_info()[0] as e:
        print(e)


def downloadLatestTxtFile(year):
    URL = f"https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.ZIP"

    os.makedirs(f"./{year}")

    try:
        with urllib.request.urlopen(URL) as zip_response:
            with ZipFile(io.BytesIO(zip_response.read())) as zfile:
                zfile.extractall(f"./{year}")
        os.remove(f"./{year}/{year}FD.xml")
        shutil.move(f"{os.getcwd()}/{year}/{year}FD.txt", ".")
        os.removedirs(f"{year}")
        os.rename(f"./{year}FD.txt", "new.txt")

    except sys.exc_info()[0] as e:
        print(e)
        return 0

    return 1


def useRedis():
    load_dotenv()
    r = redis.from_url(os.getenv("REDIS_URL"))
    old = r.get("old")
    with open("old.txt", "w") as txt_file:
        txt_file.write(old.decode())

    return r


def extractDiffToDf():
    data = {
        "first_name": [],
        "last_name": [],
        "date": [],
        "doc_id": [],
        "url": [],
    }

    base_url = "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/"
    document_id_set = set()

    with open("./old.txt", "r") as old:
        with open("./new.txt", "r") as new:
            differ = Differ()

            for line in differ.compare(old.readlines(), new.readlines()):
                if line.startswith("+"):
                    line = line.split()[1:]
                    if line[-1].startswith("2"):
                        if line[0] in ["Hon.", "Dr.", "Mr.", "Ms.", "Mrs."]:
                            line.pop(0)
                        if not line[-1] in document_id_set:
                            data["first_name"].append(line[1])
                            data["last_name"].append(line[0])
                            data["date"].append(line[-2])
                            data["doc_id"].append(line[-1])
                            data["url"].append(
                                base_url + f"{line[-2][-4:]}/{line[-1]}.pdf"
                            )
                            document_id_set.add(line[-1])

            return pd.DataFrame(data)


def updateTables(first_name, last_name, doc_id, url):
    try:
        engine = connectDb()
        with engine.connect() as conn:
            result = conn.execute(
                sqlalchemy.text(
                    f"select * from person where first_name like '%{first_name}%' and last_name like '{last_name}'"
                )
            ).all()

            if len(result):
                person_id = result[0][0]
                conn.execute(
                    sqlalchemy.text(
                        f"insert into person_to_record values ('{doc_id}', '{person_id}', '{url}')"
                    )
                )
                print(
                    f"{first_name} {last_name} was found in the DB and {doc_id} + url was added to the person_to_record table."
                )

            else:
                person_id = conn.execute(
                    sqlalchemy.text(
                        f"insert into person (first_name, last_name) values ('{first_name}', '{last_name}') returning person_id"
                    )
                ).all()[0][0]

                conn.execute(
                    sqlalchemy.text(
                        f"insert into person_to_record values ('{doc_id}', '{person_id}', '{url}')"
                    )
                )
                print(
                    f"{first_name} {last_name} was not found and added to the DB. {doc_id} + url were added to the person_to_record table."
                )

    except sys.exc_info()[0] as e:
        print(e)


def run(redis_client):
    if not downloadLatestTxtFile(datetime.today().year):
        return

    df = extractDiffToDf()

    for first_name, last_name, doc_id, url in zip(
        df["first_name"], df["last_name"], df["doc_id"], df["url"]
    ):
        updateTables(first_name, last_name, doc_id, url)

    transaction_data = extractData(df[["date", "doc_id"]])

    try:
        engine = connectDb()
        transaction_data.to_sql("record", engine, index=False, if_exists="append")

        print("Database records were updated.")
        os.remove("./old.txt")
        redis_client.delete("old")

        with open("./new.txt", "r") as new:
            redis_client.set("old", str.encode(new.read()))

        os.remove("./new.txt")

    except sys.exc_info()[0] as e:
        print(e)


def databaseMaintanence():
    try:
        engine = connectDb()
        with engine.connect() as conn:
            conn.execute(
                sqlalchemy.text(
                    "update record set company = 'ALIBABA GROUP' where company like '%ALIBABA%'"
                )
            )
            conn.execute(
                sqlalchemy.text(
                    "update record set ticker = 'TDDXX', company = 'BLF FEDFUND' where company = 'BLF FEDFUND TDDXX'"
                )
            )

        print("Database maintenance completed.")

    except sys.exc_info()[0] as e:
        print(e)
