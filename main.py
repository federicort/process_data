from collections import deque
import os
import sys
from shutil import copyfile
from datetime import timedelta, date
import logging

import sql

# Logger
# In case of error, a file is generated inside the folder in which this file is
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')
file_handler = logging.FileHandler('log_data_sen.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

# to print when using logger.debug
stream_handler = logging.StreamHandler()

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

# Initial Variables
# Titles of the section that want to be stored
titles = ('hcotas', 'dcotas', 'agua', 'energi', 'caudal', 'barra-op-costo-marginal-sing-all', 'energi-sing',
          'barra-op-costo-marginal', 'barra-transf-energi-act', 'barra-transf-energi-react')

# Paths
path = r''  # Path where the file are stored internally
path_red_cdec = r''  # Original path of the files (another server)


def last_date_bd():
    # get the date of the last data stored in the database
    # This is done because the files are not uploaded the weekends.
    query = sql.Query('database')
    last_date = query.select('data', 'max(date)',)
    last_date = str(last_date[0][0])
    query.close()
    return last_date


def copy_to_negocios(source_dir, target_dir, date_file):
    # Copy the file with the date specified in "date" from his origin to the internal folder
    date_file = date.strftime(date_file, '%y%m%d')
    source_file = os.path.join(source_dir, 'RO' + str(date_file) + '.prn')
    if os.path.isfile(source_file):
        condicion1_if = True
    else:
        condicion1_if = False
        logger.error(f'File "{source_file}" does not exist.')

    if os.path.exists(target_dir):
        condicion2_if = True
    else:
        condicion2_if = False
        logger.error(f'Path "{target_dir}" does not exist.')

    if condicion1_if and condicion2_if:
        file_name = os.path.basename(source_file)
        logger.debug(file_name)

        target = os.path.join(target_dir, file_name)
        # adding exception handling
        try:
            copyfile(source_file, target)
        except IOError as e:
            logger.error("Unable to copy file. %s" % e)
            sys.exit(1)
        except:
            logger.error("Unexpected error:", sys.exc_info())
            sys.exit(1)
    else:
        sys.exit(1)


def process(file_path, fecha):
    # process files to generate a list with the data required.
    if os.path.isfile(file_path):
        with open(file_path, "r") as file:
            arr = []
            skip = False
            read = False
            # if a any data is saved with this name it means that there was an error in the procedure
            nombre = 'Error_name'
            data_type = 'Error_name'
            for line in file:
                # deque is used to efficiently remove the first item from the list
                line = deque(line.split("    "))
                line[-1] = line[-1].replace('\n', '')
                # determine if the line is the beginning or end of a section, a name or data
                if line[0] in titles:  # If is a title
                    data_type = line[0]
                    read = True
                    skip = True
                elif line[0][:3] == 'fin':  # End of a section
                    read = False
                    skip = True
                if not skip and read:
                    if line[0] == '':
                        line.popleft()
                    if len(line) == 1 and not line[0][:1].isnumeric():  # If it is a name
                        nombre = line[0]
                    else:  # If it is data
                        for hora, dato in enumerate(line):
                            arr_temp = []
                            arr_temp.append(data_type)
                            arr_temp.append(nombre)
                            arr_temp.append(fecha)
                            arr_temp.append(str(hora))
                            arr_temp.append(str(dato))
                            arr.append(arr_temp)
                            # logger.debug(arr_temp)
                skip = False
    else:
        logger.error(f'File "{file_path}" does not exist.')
        sys.exit(1)
    return arr


def insert_data(datos_insert, date_data):
    # Delete data with the specified date and insert those found in the processed file
    query = sql.Query('database')  # Insert database name
    tabla_sql = 'table_name'  # Tabla name
    headers_insert = ['tipo', 'nombre_sen', 'fecha', 'hora', 'dato']
    sep_headers = ','
    sep_data = r"','"

    # Delete data with the specified date
    str_delete = f"delete from {tabla_sql} where fecha = '{date_data}' ;"
    query.insert_other(str_delete)

    # Insert data in database
    for item in datos_insert:
        str_headers = sep_headers.join(headers_insert)
        str_data = sep_data.join(item)
        # logger.debug(str_data)
        str_insert = f"INSERT INTO {tabla_sql} ({str_headers}) VALUES ('{str_data}') ;"
        query.insert_other(str_insert)
        # arr_medidores.append(item[1])

    query.close()
    # logger.debug(arr_medidores)
    logger.info("Data inserted in DB " + str(date_data))


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)+1):
        yield start_date + timedelta(n)


def in_fechas():
    # input date for manual data loading
    input_date_start = input('Start date in format yyyy-mm-dd: ')
    input_date_end = input('End date in format yyyy-mm-dd: ')
    year1, month1, day1 = map(int, input_date_start.split('-'))
    year2, month2, day2 = map(int, input_date_end.split('-'))
    date_start = date(year1, month1, day1)
    date_end = date(year2, month2, day2)
    # print(date_start, date_end)
    return date_start, date_end


def range_file_path(date_start=None, date_end=None):
    # Insert data of files with dates between date_start and date_end (including them)

    # If no parameters are entered
    if date_start is None and date_end is None:
        str_date_start = last_date_bd()  # last register in DB
        year, month, day = map(int, str_date_start.split('-'))
        date_start = date(year, month, day)
        date_end = date.today() - timedelta(days=1)
        if date_start >= date_end:
            date_start = date_end

    elif date_start is None or date_end is None:
        logger.error('No Start Date or End Date')
        sys.exit(1)

    if os.path.exists(path):
        for file_date in daterange(date_start, date_end):
            # format date
            srt_date = file_date.strftime('%y%m%d')
            str_date2 = file_date.strftime('%d-%m-%y')
            # copy file if not in the internal folder
            file_name = 'RO' + srt_date + '.prn'
            if file_name not in os.listdir(path):
                # If a file with the date that is being processed is not found in the internal folder,
                # it is copied from the origin folder
                copy_to_negocios(path_red_cdec, path, file_date)
            if file_name in os.listdir(path):
                # validate again if the file is in the internal folder
                for file in os.listdir(path):
                    if file[2:8] == srt_date:
                        file_path = os.path.join(path, file)
                        # run process
                        insert_data(process(file_path, str_date2), str_date2)
                        break
            else:
                logger.error(f'File "{file_name}" does not exist in : {path}')

    else:
        logger.error('Ruta "' + path + '" no existe.')
        sys.exit(1)



def run_range():
    # Go through a range of dates
    date_start, date_end = in_fechas()
    range_file_path(date_start, date_end)


def run():
    # Save from the day after the last data saved until yesterday.
    logger.info('Starting...')
    range_file_path()


if __name__ == '__main__':
    run()
