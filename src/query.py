import json
import mysql.connector
from Inspector import Inspector

def lambda_handler(event, context):


    inspector = Inspector()
    inspector.inspectCPU()
    inspector.inspectMemory()
    inspector.inspectContainer()

    connection = mysql.connector.connect(
        host="tql-db.cluster-c5oescyqc3zg.us-east-1.rds.amazonaws.com", 
        port=3306,
        user="USERNAME",
        password="PASSWORD",
        database="mobiledata"
    )

    cursor = connection.cursor()

    query = "SELECT "
    aggregations = event.get("aggregations", [])
    group_by = event.get("group", [])

    if group_by:
        query += ", ".join(group_by)
        query += ", "

    if aggregations:
        query += ", ".join([f"{agg['function']}({agg['column']}) AS {agg['function']}_{agg['column']}" for agg in aggregations])
    else:
        query += "*"
    query += " FROM data"

    filters = event.get("filters", [])
    if filters:
        query += " WHERE " + " AND ".join([f"{f['column']} = %s" for f in filters])

    group_by = event.get("group", [])
    if group_by:
        query += " GROUP BY " + ", ".join(group_by)

    values = [f["value"] for f in filters]
    print(query)
    cursor.execute(query, values)

    result = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    # Create the desired JSON format
    entries = []
    for row in result:
        entry = {}
        for i, column in enumerate(columns):
            entry[column] = row[i]
        entries.append(entry)

    inspector.addAttribute('entries', entries)

    inspector.inspectAllDeltas()

    return inspector.finish()
