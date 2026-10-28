"""Dev trial for #2223: write feed/calendar.ics under zz_dev_published/v1/, check it, remove zz_dev_published/."""

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate
from warehouse.publish import calendar

client = s3()
bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME")
calendar(client, "zz_dev_published/v1/")
head = client.head_object(Bucket=bucket, Key="zz_dev_published/v1/feed/calendar.ics")
annotate("notice", "calendar.ics", {"ContentLength": head["ContentLength"], "ContentType": head["ContentType"]})
keys = [o["Key"] for o in client.list_objects_v2(Bucket=bucket, Prefix="zz_dev_published/").get("Contents", [])]
for key in keys:
    client.delete_object(Bucket=bucket, Key=key)
annotate("notice", f"deleted {len(keys)}", {"keys": keys})
