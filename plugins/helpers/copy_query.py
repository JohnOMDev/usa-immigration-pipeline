class Copy_sql:
	copy_sql_query = ("""
    COPY {}
    FROM "s3://{}"
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    FORMAT AS {format}
    REGION {}
""")