from cassandra.cluster import Cluster


def question_1_create_table():
    question_1_drop_table_script = '''
        drop table if exists  longen_zhao_td.project_question_1;'''

    cluster = Cluster(['localhost'])
    session = cluster.connect('longen_zhao_td')

    '''station,valid,lon,lat,tmpf,dwpf,relh,drct,sknt,p01i,alti,mslp,vsby,gust,skyc1,skyc2,skyc3,skyc4,skyl1,skyl2,skyl3,skyl4,wxcodes,ice_accretion_1hr,ice_accretion_3hr,ice_accretion_6hr,peak_wind_gust,peak_wind_drct,peak_wind_time,feel,metar'''

    question_1_create_table_script = '''
        CREATE TABLE Project_Question_1 (

            station varchar,

            valid timestamp, 
            year varint, 
            month varint,
            day varint,
            hour varint,
            minute varint, 
            season varint,

            lon float,
            lat float,

            tmpf float,
            dwpf float,
            relh float,
            drct float,
            sknt float,
            p01i float,
            alti float,
            mslp float,
            vsby float,
            gust float,
            skyc1 varchar,
            skyc2 varchar,
            skyc3 varchar,
            skyc4 varchar,
            skyl1 float,
            skyl2 float,
            skyl3 float,
            skyl4 float,
            wxcodes varchar,
            ice_accretion_1hr float,
            ice_accretion_3hr float,
            ice_accretion_6hr float,
            peak_wind_gust float,
            peak_wind_drct float,
            peak_wind_time varchar,
            feel float,
            metar varchar,	
            PRIMARY KEY ((lon, lat),year, month, day, hour, minute)
        )
        '''

    session.execute(question_1_drop_table_script)
    session.execute(question_1_create_table_script)


if __name__ == "__main__":
    question_1_create_table()