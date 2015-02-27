import datetime as dt
import boto
import sys
from subprocess import call


def next_hour(date_hour):
    return date_hour + dt.timedelta(hours=1)


def hour_dir_list(start_date_hour_str, end_date_hour_str):
    start_date_hour = dt.datetime.strptime(start_date_hour_str, "%Y-%m-%d-%H")
    end_date_hour = dt.datetime.strptime(end_date_hour_str, "%Y-%m-%d-%H")
    buckets = [start_date_hour]
    cur = next_hour(start_date_hour)
    while cur <= end_date_hour:
        buckets.append(cur)
        cur = next_hour(cur)
    return [b.strftime("%Y-%m-%d-%H") for b in buckets]


def get_bad_lzo_files(bucket_name, hour_dirs):
    s3 = boto.connect_s3()
    bucket = s3.get_bucket(bucket_name)
    bad_files = []
    for h in hour_dirs:
        files = bucket.list(h)
        for f in files:
            if f.size == 0:
                bad_files.append(f.name)
    s3.close()
    return bad_files


def main():
    if len(sys.argv) < 5:
        print "USAGE: python fix_zero_byte_lzo.py S3_BUCKET START_DATE_HOUR END_DATE_HOUR ARCHIVE_HOST ARCHIVE"
        sys.exit(1)

    bucket_name = sys.argv[1]
    start_date_hour_str = sys.argv[2]
    end_date_hour_str = sys.argv[3]
    archive_host = sys.argv[4]
    archive_dir = sys.argv[5]

    hour_dirs = hour_dir_list(start_date_hour_str, end_date_hour_str)
    bad_files = get_bad_lzo_files(bucket_name, hour_dirs)

    for f in bad_files:
        print f
        name = f.rsplit("/", 1)[1]
        call(["scp", archive_host + ":" + archive_dir + "/" + name, "."])
        call(["aws", "s3", "cp", name, "s3://" + bucket_name + "/" + f])
        call(["rm", name])

    bad_files_again = get_bad_lzo_files(bucket_name, hour_dirs)
    if len(bad_files_again) > 0:
        sys.exit(1)


if __name__ == '__main__':
    main()



