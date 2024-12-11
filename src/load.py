import boto3
import time
import csv
import json
import mysql.connector
import random
from Inspector import Inspector

def lambda_handler(event, context):
    DATA_BUCKET = "tlq-experiment-load"
    BATCHSIZE = 1000
    """Main Lambda function handler"""
    # Initialize performance inspector
    inspector = Inspector()
    inspector.inspectCPU()
    inspector.inspectMemory()
    inspector.inspectContainer()
    
    s3 = boto3.client('s3')
    bucket_name = event['detail']['requestParameters']['bucketName']
    file_name = event['detail']['requestParameters']['key']
    tmp_file = f"/tmp/{file_name}"

    # Download file from S3
    s3.download_file(bucket_name, file_name, tmp_file)

    # Connect to Aurora database
    connection = mysql.connector.connect(
        host="tql-db.cluster-c5oescyqc3zg.us-east-1.rds.amazonaws.com", 
        port=3306,
        user="USERNAME",
        password="PASSWORD",
        database="mobiledata"
    )
    cursor = connection.cursor()

    # Check/Create table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS data (
        userID INTEGER AUTO_INCREMENT,
        userAge REAL, userGender TEXT, 
        userNumberOfApps INTEGER, 
        userSocialMediaUsage REAL, 
        userPercentOfSocialMedia REAL, 
        userProductivityAppUsage REAL, 
        userPercentOfProductivityAppUsage REAL, 
        userGamingAppUsage REAL, 
        userPercentOfGamingAppUsage REAL, 
        userTotalAppUsage REAL, 
        userCity TEXT, 
        resultState TEXT, 
        resultCountry TEXT, 
        PRIMARY KEY (userID)
    )
    """
    cursor.execute(create_table_sql)
    connection.commit()

    # Insert data
    insert_sql = """
    INSERT INTO data (
        userAge, userGender, userNumberOfApps, userSocialMediaUsage, 
        userPercentOfSocialMedia, userProductivityAppUsage, 
        userPercentOfProductivityAppUsage, userGamingAppUsage, 
        userPercentOfGamingAppUsage, userTotalAppUsage, userCity, 
        resultState, resultCountry
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    with open(tmp_file, 'r') as csvfile:
        reader = csv.reader(csvfile)
        rows = [tuple(row) for row in reader]

        # Process rows in batches
        for i in range(0, len(rows), BATCHSIZE):
            batch = rows[i:i + BATCHSIZE]
            cursor.executemany(insert_sql, batch)
            connection.commit()  # Commit after each batch

    connection.commit()

    inspector.inspectAllDeltas()

    filename = '/tmp/result' + str(round(time.time() * 1000)) + str(random.randint(1, 10000)) + '.json'
    with open(filename, 'w') as fp:
        json.dump(inspector.finish(), fp)

    s3.upload_file(filename, DATA_BUCKET, filename)

    return {"status": "data loaded successfully"}