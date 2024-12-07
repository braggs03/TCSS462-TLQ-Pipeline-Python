import boto3
import csv
import mysql.connector

def lambda_handler(event, context):
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
        user="admin",
        password="smLe14KRN9X7SoT3Y36V",
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

    return {"status": "data loaded successfully"}