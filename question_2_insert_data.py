import csv
import re
from cassandra.cluster import Cluster
from tqdm import tqdm

BATCH_SIZE = 10
MISSING_VALUE = 'null'


def get_session():
    cluster = Cluster(['localhost'])
    session = cluster.connect()
    session.set_keyspace('longen_zhao_td')
    return session


def read_file_gen(fname):
    dateparser = re.compile(
        '(?P<year>\d+)-(?P<month>\d+)-(?P<day>\d+) (?P<hour>\d+):(?P<minute>\d+)'
    )

    dial = csv.excel
    dial.delimiter = ','
    with open(fname) as csv_file:
        reader = csv.DictReader(csv_file, dialect=dial)
        for row in reader:

            match_valid = dateparser.match(row['valid'])

            if not match_valid:
                continue
            valid = match_valid.groupdict()
            data = dict(row)

            # set time`
            data['year'] = int(valid['year'])
            data['month'] = int(valid['month'])
            data['day'] = int(valid['day'])
            data['hour'] = int(valid['hour'])
            data['minute'] = int(valid['minute'])

            # set missing values as null
            for col in data:
                if data[col] in ('M', 'T'):
                    data[col] = MISSING_VALUE

            yield (data)


def get_insert_row_script(row: dict):
    insert_script = f'''
        insert into longen_zhao_td.project_question_2(

            station ,

            valid , 
            year , 
            month ,
            day ,
            hour ,
            minute , 

            lon ,
            lat ,

            tmpf ,
            dwpf ,
            relh ,
            drct ,
            sknt ,
            p01i ,
            alti ,
            mslp ,
            vsby ,
            gust ,
            skyc1 ,
            skyc2 ,
            skyc3 ,
            skyc4 ,
            skyl1 ,
            skyl2 ,
            skyl3 ,
            skyl4 ,
            wxcodes ,
            ice_accretion_1hr ,
            ice_accretion_3hr ,
            ice_accretion_6hr ,
            peak_wind_gust ,
            peak_wind_drct ,
            peak_wind_time ,
            feel ,
            metar 
        ) values (
            \'{row['station']}\',

            \'{row['valid']}\', 
            {row['year']}, 
            {row['month']},
            {row['day']} ,
            {row['hour']} ,
            {row['minute']} , 

            {row['lon']} ,
            {row['lat']} ,
            {row['tmpf']} ,
            {row['dwpf']} ,
            {row['relh']} ,
            {row['drct']} ,
            {row['sknt']} ,
            {row['p01i']} ,
            {row['alti']} ,
            {row['mslp']} ,
            {row['vsby']} ,
            {row['gust']} ,
            \'{row['skyc1']}\' ,
            \'{row['skyc2']}\' ,
            \'{row['skyc3']}\' ,
            \'{row['skyc4']}\' ,
            {row['skyl1']} ,
            {row['skyl2']} ,
            {row['skyl3']} ,
            {row['skyl4']} ,
            \'{row['wxcodes']}\' ,
            {row['ice_accretion_1hr']} ,
            {row['ice_accretion_3hr']} ,
            {row['ice_accretion_6hr']} ,
            {row['peak_wind_gust']} ,
            {row['peak_wind_drct']} ,
            \'{row['peak_wind_time']}\' ,
            {row['feel']} ,
            \'{row['metar']}\'
        )

    '''

    return insert_script


def insert_batch(batch, session):
    insert_batch_script = 'BEGIN BATCH\n'
    for row in batch:
        insert_batch_script += get_insert_row_script(row) + '\n'
    insert_batch_script += 'APPLY BATCH'

    session.execute(insert_batch_script)


def question_2_insert_data_with_batch(fname):
    data_generator = read_file_gen(fname)
    batch_count = 0
    batch = []
    session = get_session()

    for row in tqdm(data_generator):
        batch.append(row)
        batch_count += 1
        if batch_count % BATCH_SIZE == 0:
            insert_batch(batch, session)
            batch.clear()


if __name__ == '__main__':
    question_2_insert_data_with_batch('data/asos.txt')
    print('question 2 insert data done')