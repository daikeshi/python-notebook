import datetime as dt

import boto

DELAY_DAYS_THRESHOLD = 1
DELAY_HOURS_THRESHOLD = 2


def to_utc_timestamp(date):
    return (date - dt.datetime(1970, 1, 1)).total_seconds()


def is_delayede(process_date, start_date, end_date):
    if process_date > start_date and process_date <= end_date:
        return True
    else:
        return False


def main():
    bidder_bucket = 'mm-prod-raw-logs-bidder'

    end_date = dt.datetime(2015, 01, 26, 23)
    ndays = 15
    dates = [end_date - dt.timedelta(hours=h) for h in range(ndays * 24)]
    prefixes = [d.strftime('%Y-%m-%d-%H') for d in dates] 

    s3 = boto.connect_s3()
    bucket = s3.get_bucket(bidder_bucket)

    for prefix in prefixes:
        dir_date = dt.datetime.strptime(prefix, '%Y-%m-%d-%H')
        # start_date = dt.datetime.combine((dir_date - dt.timedelta(hours=DELAY_HOURS_THRESHOLD)), dt.time.max)

        files = bucket.list(prefix)
        for f in files:
            parts = f.name.split('.')
            if len(parts) >=3:
                process_date = dt.datetime.utcfromtimestamp(long(f.name.split('.')[-3]))
                delay = abs((dir_date - process_date).total_seconds()) / 60 / 60
                # print dir_date, process_date, dir_date.hour, process_date.hour,
                # (dir_date - process_date).seconds, delay
                if delay >= DELAY_HOURS_THRESHOLD:
                    print "%s\t%d\t%s\t%s" % (f.name, delay, dir_date, process_date)

if __name__ == '__main__':
    main()
        


