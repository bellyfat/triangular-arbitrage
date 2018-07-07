import yaml
import logging

from telegram.ext import Updater
from telegram.ext import CommandHandler

import kucoin_bot

class TelegramBot():
    def __init__(self, config_file='config.yaml'):
        with open(config_file, 'r') as f:
            self.config = yaml.load(f)

        self.TOKEN = self.config['telegram']['api_key']

        self.updater = Updater(token=self.TOKEN)
        self.j = self.updater.job_queue
        self.dispatcher = self.updater.dispatcher

        start_handler = CommandHandler('start', self.subscribe)
        ping_handler = CommandHandler('ping', self.ping)

        self.dispatcher.add_handler(start_handler)
        self.dispatcher.add_handler(ping_handler)

        self.kucoin_bot = kucoin_bot.ArbitrageBot(config_file=config_file)

    def get_chat_ids_filename(self):
        return self.config['telegram']['chat_ids_file']

    def get_chat_ids(self):
        with open(self.get_chat_ids_filename(), 'r') as f:
            content = f.read().split()
        return content

    def subscribe(self, tbot, update):
        msg_id = str(update.message.chat_id)
        try:
            if msg_id not in self.get_chat_ids():
                with open(self.get_chat_ids_filename(), 'a+') as f:
                    f.write(msg_id + ' ')

                tbot.send_message(chat_id=update.message.chat_id,
                                       text=('Welcome, now you\'ll receive'
                                             ' arbitrage oportunities'))
        except Exception as e:
            logging.error(str(e))

    def ping(self, tbot, update):
        try:
            tbot.send_message(chat_id=update.message.chat_id,
                                   text=('Pong'))
        except Exception as e:
            logging.error(str(e))

    def broadcast(self, tbot, msg):
        for id_ in self.get_chat_ids():
            try:
                tbot.send_message(chat_id=int(id_), text=msg)
            except Exception as e:
                continue

    def run_arbitrage_bot(self, tbot, job):
        arb_ops = self.kucoin_bot.run()
        if len(arb_ops) > 0:
            self.broadcast(tbot, kucoin_bot.pprint_oporunities(arb_ops))

    def run_forever(self):
        job = self.j.run_repeating(self.run_arbitrage_bot, interval=60, first=0)
        self.updater.start_polling()

if __name__ == '__main__':
    tb = TelegramBot()
    tb.run_forever()
