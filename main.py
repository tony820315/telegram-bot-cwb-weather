#!/usr/bin/env python
# -*- coding: utf-8 -*-
# This program is dedicated to the public domain under the CC0 license.

"""
Simple Bot to tell you TW weather
"""
import logging
import calendar
import forecast_36hr
import configparser
import sqlite3
import time

from datetime import datetime
from dataset import city_ids, Environmental_factors , PREDICT_CODE_MAP
from telegram.ext import Updater, CommandHandler

from timeloop import Timeloop
from datetime import timedelta

# Enable logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG)

logger = logging.getLogger(__name__)

# Load data from config.ini file
config = configparser.ConfigParser()
config.read('config.ini')

AUTH_KEY = config['TELEGRAM']['AUTH_KEY']
TOKEN = config['TELEGRAM']['TOKEN']
CWB_DB_PATH = config['CWB']['CWB_DB_PATH']

def help(bot, update):
    """Send a message when the command /help is issued."""
    update.message.reply_text('Just type, for example, /weather [city] \n ')
    update.message.reply_text('[city] = {}'.format(str(list(city_ids.values()))))

def error(bot, update, error):
    """Log Errors caused by Updates."""
    logger.warning('Update "%s" caused error "%s"', update, error)

def weather(bot, update, args):
    """Define weather at certain location"""
    text_location = "".join(str(x) for x in args)
    tw_time = calendar.timegm(time.gmtime()) + 3600*8
    result = get_recent_weather(text_location, tw_time)
    print(len(result))
    if len(result) != 0:
        update.message.reply_text(result)
    else:
        update.message.reply_text('Unknown city, please check with /help \n')


tl = Timeloop()

@tl.job(interval=timedelta(seconds=21600))
def update_cwb_data():
    print("6hr job current time : {}".format(time.ctime()))
    json_data = forecast_36hr.get_data_from_cwb('F-C0032-001', AUTH_KEY, {})
    dict_data = forecast_36hr.parse_json_to_dict_city(json_data)
    forecast_36hr.create_table_city()
    forecast_36hr.insert_data_city(dict_data)

def get_recent_weather(location, timestamp):
    conn = sqlite3.connect(CWB_DB_PATH)
    c = conn.cursor()
    result = ''
    c.execute('''SELECT * from CWB where EndTime >= ?  and Location = ?''',  (timestamp, location ))
    myresult = list(c.fetchall())
    if len(myresult) != 0:
        result += '{}{}\n '.format(location, '💭')
        for rows in list(myresult):
            result += '{} : {} ~ {} \n '.format(Environmental_factors[0], datetime.fromtimestamp(rows[0] -43200) , datetime.fromtimestamp(rows[0]))
            # result += '{} : {}, '.format(Environmental_factors[2], PREDICT_CODE_MAP[rows[2]])
            result += '{} : {} ~ {}, '.format(Environmental_factors[3], rows[4], rows[3])
            result += '{} : {}%, '.format(Environmental_factors[4], rows[5])
            result += '{} : {}\n '.format(Environmental_factors[5], rows[6])
    conn.commit()
    conn.close()

    return  result


def main():
    """Start the bot."""
    # Create the Updater and pass it your token and private key
    # updater = Updater(TOKEN, private_key=open('private.key', 'rb').read())
    updater = Updater(TOKEN)
    # Get the dispatcher to register handlers
    dp = updater.dispatcher
    # On messages that include passport data call msg
    dp.add_handler(CommandHandler("help", help))
    dp.add_handler(CommandHandler("weather", weather, pass_args=True))
    # log all errors
    dp.add_error_handler(error)
    # Start the Bot
    updater.start_polling()
    # Run the bot until you press Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT. This should be used most of the time, since
    # start_polling() is non-blocking and will stop the bot gracefully.
    updater.idle()


if __name__ == '__main__':
    update_cwb_data()
    tl.start(block=False)
    main()
    
