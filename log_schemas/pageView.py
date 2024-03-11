# pageview columns: timestamp, hashed ipAddress, hashed sessionId, pageRoute

pageViewSchema = [
    {"name": "timestamp", "type": "TIMESTAMP"},
    {"name": "sessionId", "type": "VARCHAR"},
    {"name": "pageRoute", "type": "VARCHAR"}
]