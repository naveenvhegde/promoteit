#!/usr/bin/env python
# -*- coding: utf-8 -*-


import time
import sys
import re
import logging
import pickle
import redis
import itertools

from tgbots import get_random_bot, is_admin
from telegram import TelegramError

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
logger = logging.getLogger("crosspromo")

newp = re.compile('.*(#new).*(@\w+)(.*)')
confirmp = re.compile('.*(#confirm).*(@\w+)(.*)')
sharedp = re.compile('.*(#shared).*(@\w+)(.*)')
removep = re.compile('.*(#remove).*(@\w+)(.*)')
listp = re.compile('.*(#list)(.*)')


#############################################################################


class Channel(object):
    def __init__(self, name, desc=None, count=0, date=None, stage=None):
        self.name = name.strip()
        self.desc = desc
        self.count = count
        self.date = date
        self.stage = stage
        self.strip()

    def update_stage(self, stage):
        self.stage = stage

    def log(self):
        logger.info("channel - [%s], [%s], [%d], [%s]" % (self.name, self.desc, self.count, self.stage))

    def strip(self):
        if self.name:
            self.name = self.name.strip()

        if self.desc:
            self.desc = self.desc.strip()

        if self.desc:
            self.desc = self.desc.strip()

        if self.stage:
            self.stage = self.stage.strip()

    def format(self):
        msg = "\n%s %s (%d) \n%s" % (self.stage,
                                     self.name, self.count, self.desc)
        return msg

    def raw_format(self):
        msg = "%s\n%s" % (self.name, self.desc)
        return msg


class Channels(object):
    def __init__(self):
        self.channels = {}

    def add(self, channel):
        self.channels[channel.name] = channel

    def get(self, channel):
        if self.channels.has_key(channel.name):
            return self.channels[channel.name]
        else:
            return None

    def remove(self, channel):
        if self.channels.has_key(channel.name):
            del self.channels[channel.name]
            return True
        else:
            return False

    def list(self):
        channels_list = self.channels.values()
        channels_list.sort(key=lambda x: x.count, reverse=True)
        return channels_list

    def range_list(self, low, high):
        channels_list = []

        names = self.range_names(low, high)
        for name in names:
            channels_list.append(self.channels[name])

        return channels_list

    def names(self):
        return self.channels.keys()

    def range_names(self, low, high):
        names = []

        for channel in self.list():
            if channel.count >= low and channel.count < high:
                names.append(channel.name)

        return names

    def clear(self):
        self.channels.clear()


class Database(object):
    def __init__(self):
        self.redis_key = "promo_channels"
        self.redis_archive_key = "promo_channels_archive"
        self.rdb = redis.StrictRedis(host='localhost', port=6379, db=0)

    def load(self):
        raw = self.rdb.get(self.redis_key)
        if raw:
            return pickle.loads(raw)
        else:
            return Channels()

    def store(self, channels):
        self.rdb.set(self.redis_key, pickle.dumps(channels))

    def archive(self, channels):
        self.rdb.set(self.redis_archive_key, pickle.dumps(channels))
        

#############################################################################


db = Database()
channels = db.load()


def refresh_channel_from_telegram(channel, bot=None):
    tgbot = bot
    if tgbot is None:
        tgbot = get_random_bot()

    try:
        channel.count = tgbot.getChatMembersCount(chat_id=channel.name)
        channel.name = "@%s" % tgbot.getChat(channel.name).username
    except TelegramError as tgerr:
        logger.error(tgerr)

    return channel


def on_new_channel(bot, update, channel):
    channel = refresh_channel_from_telegram(channel)

    channels.remove(channel)
    channels.add(channel)
    db.store(channels)

    logger.info("on_new_channel - #added %s" % channel.name)
    channel.log()

    update.message.reply_text(text="#added %s" % channel.name)


def on_confirm_channel(bot, update, channel):
    channel = refresh_channel_from_telegram(channel)

    existing = channels.get(channel)
    if existing is None:
        logger.info("on_confirm_channel - #notfound %s" % channel.name)
        update.message.reply_text(text="#notfound %s" % channel.name)
        return

    existing.update_stage("#confirm")
    existing.log()
    db.store(channels)
    logger.info("on_confirm_channel - #confirmed %s" % channel.name)

    update.message.reply_text(text="#confirmed %s" % channel.name)


def on_shared_channel(bot, update, channel):
    channel = refresh_channel_from_telegram(channel)

    existing = channels.get(channel)
    if existing is None:
        logger.info("on_shared_channel - #notfound %s" % channel.name)
        update.message.reply_text(text="#notfound %s" % channel.name)
        return

    existing.update_stage("#shared")
    existing.log()
    db.store(channels)
    logger.info("on_shared_channel - #shared %s" % channel.name)

    update.message.reply_text(text="#shared %s" % channel.name)


def on_remove_channel(bot, update, channel):
    channel = refresh_channel_from_telegram(channel)

    removed = channels.remove(channel)
    if removed is False:
        logger.info("on_remove_channel - #notfound %s" % channel.name)
        update.message.reply_text(text="#notfound %s" % channel.name)
        return

    db.store(channels)

    logger.info("on_remove_channel - #removed %s" % channel.name)
    update.message.reply_text(text="#removed %s" % channel.name)


def on_list_channel_names(bot, update, type, low, high):
    logger.info("on_list_channel_names - [%s][%d][%d]" % (type, low, high))

    channel_names = channels.range_names(low, high)

    text = ""
    for name in channel_names:
        text += name + "\n"
    text += "\n#%s #%dchannels" % (type, len(channel_names))

    logger.info("\n%s" % text)
    update.message.reply_text(text=text)


def on_list_channels(bot, update, type, low, high):
    logger.info("on_list_channels - [%s][%d][%d]" % (type, low, high))

    channels_list = channels.range_list(low, high)

    text = ""
    for channel in channels_list:
        text += channel.format() + "\n"
    text += "\n#%s #%dchannels" % (type, len(channels_list))

    logger.info("\n%s" % text)
    update.message.reply_text(text=text)


def on_refresh_channels(bot, update):
    logger.info("on_refresh_channels %s" % "#refreshing")
    update.message.reply_text(text="#refreshing")

    channels_list = channels.list()
    for channel in channels_list:
        channel.count = bot.getChatMembersCount(chat_id=channel.name)

    db.store(channels)

    logger.info("on_refresh_channels %s" % "#refreshed")

    update.message.reply_text(text="#refreshed")
    on_list_channels(bot, update)


def handle_message(bot, update, text):
    text = " ".join(text.split('\n'))

    logger.info("handle_message - %s" % text)

    m = newp.match(text)
    if m:
        channel = Channel(name=m.group(2), desc=m.group(3), stage=m.group(1))
        return on_new_channel(bot, update, channel)

    m = confirmp.match(text)
    if m:
        channel = Channel(name=m.group(2), desc=m.group(3), stage=m.group(1))
        return on_confirm_channel(bot, update, channel)

    m = sharedp.match(text)
    if m:
        channel = Channel(name=m.group(2), stage=m.group(1))
        return on_shared_channel(bot, update, channel)

    m = removep.match(text)
    if m:
        channel = Channel(name=m.group(2), stage=m.group(1))
        return on_remove_channel(bot, update, channel)


def split_text(text):
    for hashtag in re.findall('.*(#\w+).*', text):
        text = text.replace(hashtag, hashtag.lower())

    texts = []

    if "#new" in text:
        for s in text.split("#new"):
            if len(s) > 0:
                s = "#new " + s
                texts.append(s)

    elif "#confirm" in text:
        for s in re.findall(".*(\@\w+).*", text):
            s = "#confirm " + s
            texts.append(s)

    elif "#shared" in text:
        for s in re.findall(".*(\@\w+).*", text):
            s = "#shared " + s
            texts.append(s)

    elif "#remove" in text:
        for s in re.findall(".*(\@\w+).*", text):
            s = "#remove " + s
            texts.append(s)

    else:
        texts.append(text)

    return texts


def on_message(bot, update):
    if not is_admin(update):
        return

    for text in split_text(update.message.text):
        handle_message(bot, update, text)
        time.sleep(1)


def on_start_command(bot, update):
    logger.info("from %s", update.message.from_user.username)
    if not is_admin(update):
        return

    global channels
    channels = db.load()

    text = "Hey, welcome \n\n" \
           "#new <name> <desc>\n" \
           "#confirm <name> \n" \
           "#shared <name> \n" \
           "#remove <name> \n\n" \
           "/list_all \n" \
           "/list_all_names \n\n" \
           "/list_0_500 \n" \
           "/list_0_500_names \n" \
           "/list_0_500_confirmed \n" \
           "/list_0_500_notconfirmed \n" \
           "/list_0_500_final \n\n" \
           "/list_500_1000 \n" \
           "/list_500_1000_names \n" \
           "/list_500_1000_confirmed \n" \
           "/list_500_1000_notconfirmed \n" \
           "/list_500_1000_final \n\n" \
           "/list_1000_5000 \n" \
           "/list_1000_5000_names \n" \
           "/list_1000_5000_confirmed \n" \
           "/list_1000_5000_notconfirmed \n" \
           "/list_1000_5000_final \n\n" \
           "/list_5000_plus \n" \
           "/list_5000_plus_names \n" \
           "/list_5000_plus_confirmed \n" \
           "/list_5000_plus_notconfirmed \n" \
           "/list_5000_plus_final \n"

    update.message.reply_text(text=text)


def on_refresh_command(bot, update):
    if not is_admin(update):
        return
    on_refresh_channels(bot, update)


def on_list_all_command(bot, update):
    if not is_admin(update):
        return
    on_list_channels(bot, update, "all", 0, 1000000)


def on_list_all_names_command(bot, update):
    if not is_admin(update):
        return
    on_list_channel_names(bot, update, "all", 0, 1000000)


def on_list_0_500_command(bot, update):
    if not is_admin(update):
        return
    on_list_channels(bot, update, "0_500_list", 0, 500)


def on_list_0_500_names_command(bot, update):
    if not is_admin(update):
        return
    on_list_channel_names(bot, update, "0_500_names", 0, 500)


def on_list_0_500_confirmed_command(bot, update):
    if not is_admin(update):
        return
    on_list_confirmed_channels(bot, update, "0_500_list", 0, 500)


def on_list_0_500_notconfirmed_command(bot, update):
    if not is_admin(update):
        return
    on_list_not_confirmed_channels(bot, update, "0_500_list", 0, 500)


def on_list_0_500_final_command(bot, update, args):
    if not is_admin(update):
        return
    on_list_final(bot, update, "0_500_list", 0, 500, args)


def on_list_500_1000_command(bot, update):
    if not is_admin(update):
        return
    on_list_channels(bot, update, "500_1000_list", 500, 1000)


def on_list_500_1000_names_command(bot, update):
    if not is_admin(update):
        return
    on_list_channel_names(bot, update, "500_1000_names", 500, 1000)


def on_list_500_1000_confirmed_command(bot, update):
    if not is_admin(update):
        return
    on_list_confirmed_channels(bot, update, "500_1000_list", 500, 1000)


def on_list_500_1000_notconfirmed_command(bot, update):
    if not is_admin(update):
        return
    on_list_not_confirmed_channels(bot, update, "500_1000_list", 500, 1000)


def on_list_500_1000_final_command(bot, update, args):
    if not is_admin(update):
        return
    on_list_final(bot, update, "500_1000_list", 500, 1000, args)


def on_list_1000_5000_command(bot, update):
    if not is_admin(update):
        return
    on_list_channels(bot, update, "1000_5000_list", 1000, 5000)


def on_list_1000_5000_names_command(bot, update):
    if not is_admin(update):
        return
    on_list_channel_names(bot, update, "1000_5000_names", 1000, 5000)


def on_list_1000_5000_confirmed_command(bot, update):
    if not is_admin(update):
        return
    on_list_confirmed_channels(bot, update, "1000_5000_list", 1000, 5000)


def on_list_1000_5000_notconfirmed_command(bot, update):
    if not is_admin(update):
        return
    on_list_not_confirmed_channels(bot, update, "1000_5000_list", 1000, 5000)


def on_list_1000_5000_final_command(bot, update, args):
    if not is_admin(update):
        return
    on_list_final(bot, update, "1000_5000_list", 1000, 5000, args)


def on_list_5000_plus_command(bot, update):
    if not is_admin(update):
        return
    on_list_channels(bot, update, "5000_plus_list", 5000, 1000000)


def on_list_5000_plus_names_command(bot, update):
    if not is_admin(update):
        return
    on_list_channel_names(bot, update, "5000_plus_names", 5000, 1000000)


def on_list_5000_plus_confirmed_command(bot, update):
    if not is_admin(update):
        return
    on_list_confirmed_channels(bot, update, "5000_plus_list", 5000, 1000000)


def on_list_5000_plus_notconfirmed_command(bot, update):
    if not is_admin(update):
        return
    on_list_not_confirmed_channels(bot, update, "5000_plus_list", 5000, 1000000)


def on_list_5000_plus_final_command(bot, update, args):
    if not is_admin(update):
        return
    on_list_final(bot, update, "5000_plus_list", 5000, 1000000, args)


def on_list_confirmed_channels(bot, update, type, low, high):
    logger.info("on_list_confirmed_channels")

    channels_list = filter(lambda x: x.stage == "#confirm", channels.range_list(low, high))

    text = ""
    for channel in channels_list:
        text += channel.name + "\n"
    text += "\n#%s %s #%dchannels" % (type, "#confirmed", len(channels_list))

    logger.info("\n%s" % text)
    update.message.reply_text(text=text)


def on_list_not_confirmed_channels(bot, update, type, low, high):
    logger.info("on_list_not_confirmed_channels")

    channels_list = filter(lambda x: x.stage != "#confirm", channels.range_list(low, high))

    text = ""
    for channel in channels_list:
        text += channel.name + "\n"
    text += "\n#%s %s #%dchannels" % (type, "#notconfirmed", len(channels_list))

    logger.info("\n%s" % text)
    update.message.reply_text(text=text)


def grouper(n, iterable, fillvalue=None):
    args = [iter(iterable)] * n
    return itertools.izip_longest(fillvalue=fillvalue, *args)


def on_list_final(bot, update, type, low, high, args):
    if len(args) < 1:
        update.message.reply_text("<command< <no_of_list> <emojis...>")
        return

    no = int(args[0])
    emojis = args[1:]

    if len(emojis) < no:
        update.message.reply_text("specified [%d] lists, but only [%d] emojis" % (no, len(emojis)))
        return

    channels_list = filter(lambda x: x.stage == "#confirm", channels.range_list(low, high))

    message = "splitting [%d] channels into [%d] lists" % (len(channels_list), no)
    logger.info("on_split_list: %s", message)
    update.message.reply_text(message)

    final_channels_list = []
    for list in range(0, no):
        final_channels_list.append([])

    flip = False
    for i in grouper(no, channels_list):
        elements = None
        if flip:
            elements = i[::-1]
            flip = False
        else:
            elements = i
            flip = True

        for e in range(0, len(elements)):
            if elements[e] is not None:
                final_channels_list[e].append(elements[e])

    for i in range(0, len(final_channels_list)):
        channels_list = final_channels_list[i]
        text = u"🗣 Best channels you should join today. \n Here is the list👇 \n\n"
        for channel in channels_list:
            text += emojis[i] + " " + channel.raw_format() + "\n\n"
        text += u"_____________________\n JOIN TO PROMOTE YOUR CHANNEL \n➡️  @promote_it\n"

        text += "\n#%s #list%d #%dchannels #%dreach" % (type, i+1, len(channels_list), sum(c.count for c in channels_list))
        logger.info("\n%s" % text)
        update.message.reply_text(text)


def error(bot, update, error):
    logger.warn('update "%s" caused error "%s"' % (update, error))
    update.message.reply_text(text=error)


#############################################################################

def start_bot():
    from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
    from tgbots import bot_token

    updater = Updater(bot_token)

    logger.info(updater.bot.getMe())

    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", on_start_command))

    dp.add_handler(CommandHandler("list_all", on_list_all_command))
    dp.add_handler(CommandHandler("list_all_names", on_list_all_names_command))

    dp.add_handler(CommandHandler("list_0_500", on_list_0_500_command))
    dp.add_handler(CommandHandler("list_0_500_names", on_list_0_500_names_command))
    dp.add_handler(CommandHandler("list_0_500_confirmed", on_list_0_500_confirmed_command))
    dp.add_handler(CommandHandler("list_0_500_notconfirmed", on_list_0_500_notconfirmed_command))
    dp.add_handler(CommandHandler("list_0_500_final", on_list_0_500_final_command, pass_args=True))

    dp.add_handler(CommandHandler("list_500_1000", on_list_500_1000_command))
    dp.add_handler(CommandHandler("list_500_1000_names", on_list_500_1000_names_command))
    dp.add_handler(CommandHandler("list_500_1000_confirmed", on_list_500_1000_confirmed_command))
    dp.add_handler(CommandHandler("list_500_1000_notconfirmed", on_list_500_1000_notconfirmed_command))
    dp.add_handler(CommandHandler("list_500_1000_final", on_list_500_1000_final_command, pass_args=True))

    dp.add_handler(CommandHandler("list_1000_5000", on_list_1000_5000_command))
    dp.add_handler(CommandHandler("list_1000_5000_names", on_list_1000_5000_names_command))
    dp.add_handler(CommandHandler("list_1000_5000_confirmed", on_list_1000_5000_confirmed_command))
    dp.add_handler(CommandHandler("list_1000_5000_notconfirmed", on_list_1000_5000_notconfirmed_command))
    dp.add_handler(CommandHandler("list_1000_5000_final", on_list_1000_5000_final_command, pass_args=True))

    dp.add_handler(CommandHandler("list_5000_plus", on_list_5000_plus_command))
    dp.add_handler(CommandHandler("list_5000_plus_names", on_list_5000_plus_names_command))
    dp.add_handler(CommandHandler("list_5000_plus_confirmed", on_list_5000_plus_confirmed_command))
    dp.add_handler(CommandHandler("list_5000_plus_notconfirmed", on_list_5000_plus_notconfirmed_command))
    dp.add_handler(CommandHandler("list_5000_plus_final", on_list_5000_plus_final_command, pass_args=True))

    dp.add_handler(CommandHandler("clean_channels", clean_channels))

    dp.add_handler(MessageHandler(Filters.text, on_message))
    dp.add_error_handler(error)

    updater.start_polling()

    updater.idle()


def clean_channels():
    if not is_admin(update):
        return

    logger.info("clean_channels")

    db.archive(channels)
    channels.clear()
    db.store(channels)

    logger.info("clean_channels done")

def refresh_count():
    logger.info("#refreshing")

    channels_list = channels.list()

    for i in range(0, len(channels_list)):
        bot = get_random_bot(i)
        channel = channels_list[i];
        channel = refresh_channel_from_telegram(channel, bot)
        logger.info(i)
        channel.log()
        time.sleep(2)

    db.store(channels)

    logger.info("#refreshed")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        logger.error("require arguments")
        sys.exit()

    input = sys.argv[1]

    if input == "start":
        start_bot()
    elif input == "clean":
        clean_channels()
    elif input == "refresh":
        refresh_count()
