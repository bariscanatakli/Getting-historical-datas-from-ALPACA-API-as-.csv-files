import config  # importing config.py in same folder

import requests
import json
import csv
import os


symbolThatYouWant = "A"  # symbol that you want to creating

list_letter = config.list_A  # you gotta do it same letter with symbol that you wanted

# example
# symbolThatYouWant = "C"
# list_letter = config.list_C


try:
    os.mkdir("results")

except FileExistsError:
    pass

date_list = []  # list of dates, to configure how many that we want to write
time_list = []  # list of times, to memorize our data's count
data_list = []  # list of datas, this is data of our data's :D


print("Creating files starting with ", symbolThatYouWant) # printing symbol's name


def create_file(dataFromJson, numberOfCsvFile, r): # Creating folder calling with results and creating files as .csv

    print('{}{} is creating.'.format(dataFromJson, numberOfCsvFile))

    f = open(os.getcwd()+"\\results\\" +
             "{}{}.csv".format(dataFromJson, numberOfCsvFile), "w")
    
    f.write(json.dumps(r.json(), indent=4)) # writing data as .json format. We will read them and we will write them .csv


def hours_to_minutes(value):  # converting hours to minutes.

    return (int(str(value[:2]))*60) + int(str(value[2:]))


def utc_to_ny(time):  # converting utc to ny time If you want to make original just delete that code and make it 'pass'
    # or just make them comment codes

    if time//60 >= 5:

        if time % 60 == 0:

            time = str((time//60)-5) + '00'

        else:

            time = str((time//60)-5) + '30'

    else:

        if time % 60 == 0:

            time = str((time//60)+19) + '00'

        else:

            time = str((time//60)+19) + '30'

    if len(time) == 3:

        time = '0' + time

    return time


def data_list_create(i):  # returning a row for csv writer
    return [dataFromJson['symbol'], get_date(i), hours_to_minutes(get_time(i)), i.get(  # we are using hours_to_minutes func for writing a row in csv
        'o'), i.get('h'), i.get('l'), i.get('c'), i.get('v')]


def create_row(minus):  # creating row as u can see
    if minus > 1:  # we are automatically writing data if there any missing data. Also we are searching data's time variable for that
        # for example: if first data's time variable is 900 and second data's time variable is 1030 we will do automatically copy first data and creating 2 copy
        # we are creating 2 copy because (1030-900)/30 = 3, but third row will be writen by next command block
        # We are gonna do make changes a bit tho, we have got 900 and 1030 time variable so, we will do copy data's time variable 930 and 1000 because 3th row (1030) will be written
        # After that we will be written other 2 copys of them in .csv file
        # We did it now we filled all of them data

        for d in range(0, minus):

            if date_list.count(data_list[counter][1]) < 15 and data_list[counter-1][1] == data_list[counter][1]:

                writerCSV.writerow([data_list[counter][0], data_list[counter][1],
                                    utc_to_ny(
                    ((int(data_list[counter][2]))+30*d)), data_list[counter][3], data_list[counter][4],
                    data_list[counter][5], data_list[counter][6], data_list[counter][7]])

                date_list.append(data_list[counter][1])

                time_list.append([data_list[counter][1], utc_to_ny(
                    ((int(data_list[counter][2]))+30*d))])

    if minus == 1:  # if there arent wrong this is working.
        if date_list.count(data_list[counter][1]) < 15:

            writerCSV.writerow([data_list[counter][0], data_list[counter][1],
                                utc_to_ny(
                (int(data_list[counter][2]))), data_list[counter][3], data_list[counter][4],
                data_list[counter][5], data_list[counter][6], data_list[counter][7]])

            date_list.append(data_list[counter][1])

            time_list.append([data_list[counter][1], utc_to_ny(
                int(data_list[counter+1][2]))])


def get_time(i):
    # we are getting data that is being date and time from .json file that we created at create_file() func
    date_and_time = i.get('t')

    # we are parsing our date_and_time variable to just time variable
    new_time = date_and_time[11:16]

    # we are also parsing like 20:00 to 2000
    last_time = new_time.replace(':', "")

    return last_time


def get_date(i):
    # we are getting data that is being date and time from .json file that we created at create_file() func
    date_and_time = i.get('t')

    # we are parsing our date_and_time variable to just date variable
    new_date = date_and_time[:10]

    last_date = new_date.replace('-', "")

    return last_date


# creating files function with code 1---
for symbol in list_letter:  # iterator for symbols that we wanted

    if os.path.isfile(os.getcwd()+"\\results\\{}\\".format(symbolThatYouWant)+"{}.csv".format(symbol)) == False:  # is file is there???

        day_bars_url_1 = '{}/{}/bars?start={}&end={}&limit={}&timeframe={}'.format(
            config.BARS_URL, symbol, '2019-01-01', '2020-06-01', '10000', '30Min')  # getting api data url 1

        day_bars_url_2 = '{}/{}/bars?start={}&end={}&limit={}&timeframe={}'.format(
            config.BARS_URL, symbol, '2020-06-02', '2021-09-01', '10000', '30Min')  # getting api data url 2

        try:

            r_1 = requests.get(
                day_bars_url_1, headers=config.HEADERS)  # getting api data 1

            r_2 = requests.get(
                day_bars_url_2, headers=config.HEADERS)  # getting api data 2

        except BaseException as error:

            print('An exception occurred: {}'.format(error))

            continue

        create_file(symbol, 1, r_1)  # function that is creating files

        dataFromAlpaca = open(os.getcwd()+"\\results\\" +
                              "{}1.csv".format(symbol), "r")  # data is coming from alpaca api

        # data is coming from alpaca api
        dataFromJson = json.load(dataFromAlpaca)

        dataFromAlpaca.close()  # closing :D

        # we removed first copy
        os.remove(os.getcwd()+"\\results\\"+"{}1.csv".format(symbol))

        try:
            # making directory
            os.mkdir(os.getcwd()+"\\results\\{}".format(symbolThatYouWant))
        except FileExistsError as err:
            pass

        with open(os.getcwd()+"\\results\\{}\\".format(symbolThatYouWant)+"{}.csv".format(symbol), 'w', newline="") as FileCSV:

            writerCSV = csv.writer(FileCSV, delimiter=',',
                                   quotechar='"', quoting=csv.QUOTE_MINIMAL)

            writerCSV.writerow(['Symbol', 'Date', 'Time', 'Open', 'High',
                                'Low', 'Close', 'Volume'])  # making row

            try:
                for i in dataFromJson['bars']:

                    # being sure that is correct symbol
                    if symbol == dataFromJson['symbol']:

                        # appending data to data_list
                        data_list.append(data_list_create(i))

            except TypeError:

                print('{}.csv Can not loading.'.format(symbol))

            counter = 0  # We are counting because we will learn old data and new data also recent data to configure our algorythm

            try:
                for i in dataFromJson['bars']:  # every bars in .json data

                    # being sure is it true symbol
                    if dataFromJson['symbol'] == data_list[counter][0]:

                        time = hours_to_minutes(get_time(i))  # time :D

                        try:  # we are iterating in data_list to compare next day with today

                            if get_date(i) == data_list[counter][1]:

                                # if there are 30 min difference or bigger
                                if data_list[counter+1][2] - data_list[counter][2] >= 30:

                                    minus = (data_list[counter+1][2] -                    # we are getting minus variable
                                             data_list[counter][2])//30                    # and we are using minus variable at create_row(minus) function

                                    if date_list.count(get_time(i)) < 15 and time >= 840 and time <= 1260:

                                        create_row(minus)

                        except IndexError:  # error

                            if get_date(i) == data_list[counter][1]:

                                if data_list[-1][2] - data_list[counter][2] >= 30:

                                    minus = (data_list[-1][2] -
                                             data_list[counter][2])//30

                                    if date_list.count(get_time(i)) < 15 and time >= 840 and time <= 1260:

                                        create_row(minus)

                        # -------------------------------------------
                        try:  # if our data's last parts are missing that section is combining them to 15 period time
                            # for example if is there 11 part row at one day, that section under the below is making 4 row to reach 15 period at one day

                            # every part's time variable should be different among them so we are increasing them every row +30
                            counter_while = -30

                            # if last row that we wrote's date isnt same new day's day
                            if data_list[counter][1] != data_list[counter+1][1]:

                                # making copies until we have 15 period at one day
                                while date_list.count(date_list[-1]) < 15:

                                    counter_while += 30

                                    writerCSV.writerow([data_list[counter][0], data_list[counter][1],
                                                        utc_to_ny(int(
                                                            data_list[counter][2])+counter_while), data_list[counter][3], data_list[counter][4],
                                                        data_list[counter][5], data_list[counter][6], data_list[counter][7]])

                                    date_list.append(date_list[-1])

                                    time_list.append([data_list[counter][1], utc_to_ny(int(
                                        data_list[counter][2])+counter_while)])

                                date_list = []

                        except IndexError:

                            pass

                        counter += 1

                counter = 0

                data_list = []

            except TypeError:

                pass

        create_file(symbol, 2, r_2)  # function that is creating files

        dataFromAlpaca = open(os.getcwd()+"\\results\\" +
                              "{}2.csv".format(symbol), "r")  # data is coming from alpaca api

        # data is coming from alpaca api
        dataFromJson = json.load(dataFromAlpaca)

        dataFromAlpaca.close()  # closing :D

        # we removed second copy
        os.remove(os.getcwd()+"\\results\\"+"{}2.csv".format(symbol))

        with open(os.getcwd()+"\\results\\{}\\".format(symbolThatYouWant)+"{}.csv".format(symbol), 'a', newline="") as FileCSV:

            writerCSV = csv.writer(FileCSV, delimiter=',',
                                   quotechar='"', quoting=csv.QUOTE_MINIMAL)

            try:

                for i in dataFromJson['bars']:

                    # being sure that is correct symbol
                    if symbol == dataFromJson['symbol']:

                        # appending data to data_list
                        data_list.append(data_list_create(i))

            except TypeError:

                print('{}.csv Yüklenemiyor.'.format(symbol))

            counter = 0  # We are counting because we will learn old data and new data also recent data to configure our algorythm
            try:
                for i in dataFromJson['bars']:  # every bars in .json data

                    # being sure is it true symbol
                    if dataFromJson['symbol'] == data_list[counter][0]:

                        time = hours_to_minutes(get_time(i))  # time :D

                        try:  # we are iterating in data_list to compare next day with today

                            if get_date(i) == data_list[counter][1]:

                                # if there are 30 min difference or bigger
                                if data_list[counter+1][2] - data_list[counter][2] >= 30:

                                    minus = (data_list[counter+1][2] -                    # we are getting minus variable
                                             data_list[counter][2])//30                    # and we are using minus variable at create_row(minus) function

                                    if date_list.count(get_time(i)) < 15 and time >= 840 and time <= 1260:

                                        create_row(minus)

                        except IndexError:

                            if get_date(i) == data_list[counter][1]:

                                if data_list[-1][2] - data_list[counter][2] >= 30:

                                    minus = (data_list[-1][2] -
                                             data_list[counter][2])//30

                                    if date_list.count(get_time(i)) < 15 and time >= 840 and time <= 1260:
                                        create_row(minus)
                        # -------------------------------------------
                        try:  # if our data's last parts are missing that section is combining them to 15 period time
                            # for example if is there 11 part row at one day, that section under the below is making 4 row to reach 15 period at one day

                            # every part's time variable should be different among them so we are increasing them every row +30
                            counter_while = -30

                            # if last row that we wrote's date isnt same new day's day
                            if data_list[counter][1] != data_list[counter+1][1]:

                                # making copies until we have 15 period at one day
                                while date_list.count(date_list[-1]) < 15:

                                    counter_while += 30

                                    writerCSV.writerow([data_list[counter][0], data_list[counter][1],
                                                        utc_to_ny(int(
                                                            data_list[counter][2])+counter_while), data_list[counter][3], data_list[counter][4],
                                                        data_list[counter][5], data_list[counter][6], data_list[counter][7]])

                                    date_list.append(date_list[-1])

                                    time_list.append([data_list[counter][1], utc_to_ny(int(
                                        data_list[counter][2])+counter_while)])

                                date_list = []

                        except IndexError:

                            pass

                        counter += 1

                counter = 0

                data_list = []

                print(symbol + " file is created")
            except TypeError:

                pass


# Made by bariscanatakli
