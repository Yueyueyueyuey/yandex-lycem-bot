import os
import sqlite3
import time
import math
import logging
import datetime

import pytz

from db import create_chats_db


def load_locale_string(db_path: str, chat: str):
	conn = sqlite3.connect(os.path.join(db_path, 'launchbot-data.db'))
	cursor = conn.cursor()

	try:
		cursor.execute('SELECT timezone_str FROM chats WHERE chat = ?',
			(chat, ))
	except sqlite3.OperationalError:
		return None

	query_return = cursor.fetchall()
	if len(query_return) == 0:
		return None

	return query_return[0][0]


def remove_time_zone_information(db_path: str, chat: str):
	conn = sqlite3.connect(os.path.join(db_path, 'launchbot-data.db'))
	cursor = conn.cursor()

	try:
		cursor.execute(
			'UPDATE chats SET time_zone_str = ?, time_zone = ? WHERE chat = ?',
			(None, None, chat))

	except:
		logging.exception(
			f'ошибка обновления бд'
		)

	conn.commit()
	conn.close()


def update_time_zone_string(db_path: str, chat: str, time_zone: str):
	conn = sqlite3.connect(os.path.join(db_path, 'launchbot-data.db'))
	cursor = conn.cursor()

	try:
		cursor.execute(
			'''INSERT INTO chats (chat, subscribed_since, time_zone, time_zone_str,
				command_permissions, postpone_notify, notify_time_pref, enabled_notifications, 
				disabled_notifications) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
			(chat, int(
			time.time()), None, time_zone, None, None, '1,1,1,1', None, None))
	except sqlite3.IntegrityError:
		cursor.execute(
			"UPDATE chats SET time_zone_str = ?, time_zone = ? WHERE chat = ?",
			(time_zone, None, chat))

	conn.commit()
	conn.close()



def update_time_zone_value(db_path: str, chat: str, offset: str):
	conn = sqlite3.connect(os.path.join(db_path, 'launchbot-data.db'))
	cursor = conn.cursor()

	if 'h' in offset:
		offset = int(offset.replace('h', ''))
	elif 'm' in offset:
		offset = float(int(offset.replace('m', '')) / 60)

	current_value = load_time_zone_status(db_path, chat, False)
	current_value = 0 if current_value is None else current_value
	new_time_zone_value = current_value + offset

	if new_time_zone_value > 14:
		new_time_zone_value = -12
	elif new_time_zone_value < -12:
		new_time_zone_value = 14

	try:
		cursor.execute(
			'''INSERT INTO chats (chat, subscribed_since, time_zone, time_zone_str,
				command_permissions, postpone_notify, notify_time_pref, enabled_notifications, 
				disabled_notifications) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
			(chat, int(time.time()), new_time_zone_value, None, None, None,
			'1,1,1,1', None, None))
	except:
		cursor.execute(
			"UPDATE chats SET time_zone = ?, time_zone_str = ? WHERE chat = ?",
			(new_time_zone_value, None, chat))

	conn.commit()
	conn.close()


def load_time_zone_status(data_dir: str, chat: str, readable: bool):
	conn = sqlite3.connect(os.path.join(data_dir, 'launchbot-data.db'))
	cursor = conn.cursor()

	try:
		cursor.execute(
			"SELECT time_zone, time_zone_str FROM chats WHERE chat = ?",
			(chat, ))
	except:
		create_chats_db(db_path=data_dir, cursor=cursor)
		conn.commit()
		cursor.execute(
			"SELECT time_zone, time_zone_str FROM chats WHERE chat = ?",
			(chat, ))

	query_return = cursor.fetchall()
	conn.close()

	if len(query_return) != 0:
		time_zone_string_found = bool(query_return[0][1] is not None)

	if not readable:
		if len(query_return) == 0:
			return 0

		if not time_zone_string_found:
			if query_return[0][0] is None:
				return 0

			return float(query_return[0][0])

		timezone = pytz.timezone(query_return[0][1])
		user_local_now = datetime.datetime.now(timezone)
		utc_offset = user_local_now.utcoffset().total_seconds() / 3600
		return utc_offset

	if len(query_return) == 0:
		return '+0'

	if not time_zone_string_found:
		if query_return[0][0] is None:
			return '+0'

		status = float(query_return[0][0])

		mins = int(60 * (abs(status) % 1))
		hours = math.floor(status)
		prefix = '+' if hours >= 0 else ''

		return f'{prefix}{hours}' if mins == 0 else f'{prefix}{hours}:{mins}'

	timezone = pytz.timezone(query_return[0][1])
	user_local_now = datetime.datetime.now(timezone)
	user_utc_offset = user_local_now.utcoffset().total_seconds() / 3600

	if user_utc_offset % 1 == 0:
		user_utc_offset = int(user_utc_offset)
		utc_offset_str = f'+{user_utc_offset}' if user_utc_offset >= 0 else f'{user_utc_offset}'
	else:
		utc_offset_hours = math.floor(user_utc_offset)
		utc_offset_minutes = int((user_utc_offset % 1) * 60)
		utc_offset_str = f'{utc_offset_hours}:{utc_offset_minutes}'
		utc_offset_str = f'+{utc_offset_str}' if user_utc_offset >= 0 else f'{utc_offset_str}'

	return utc_offset_str


def load_bulk_tz_offset(data_dir: str, chat_id_set: set):
	conn = sqlite3.connect(os.path.join(data_dir, 'launchbot-data.db'))
	conn.row_factory = sqlite3.Row
	cursor = conn.cursor()

	if len(chat_id_set) == 0:
		conn.close()
		return {}
	chats_str, set_len = '', len(chat_id_set)
	for enum, chat_id in enumerate(chat_id_set):
		chats_str += f"'{chat_id}'"
		if enum < set_len - 1:
			chats_str += ','
	try:
		cursor.execute(
			f'SELECT chat, time_zone, time_zone_str FROM chats WHERE chat IN ({chats_str})'
		)
	except sqlite3.OperationalError:
		return {}

	query_return = cursor.fetchall()
	conn.close()

	if len(query_return) == 0:
		raise Exception(
			'Error pulling time zone information from chats database!')

	tz_offset_dict = {}
	for chat_row in query_return:
		time_zone_string_found = bool(chat_row['time_zone_str'] is not None)

		if not time_zone_string_found:
			if chat_row['time_zone'] is None:
				tz_offset_dict[chat_row['chat']] = (float(0), '+0')
				continue

			tz_offset = float(chat_row['time_zone'])

			mins, hours = int(60 * (abs(tz_offset) % 1)), math.floor(tz_offset)
			prefix = '+' if hours >= 0 else ''
			tz_str = f'{prefix}{hours}' if mins == 0 else f'{prefix}{hours}:{mins}'

			tz_offset_dict[chat_row['chat']] = (tz_offset, tz_str)
			continue

		timezone = pytz.timezone(chat_row['time_zone_str'])
		local_now = datetime.datetime.now(timezone)
		utc_offset = local_now.utcoffset().total_seconds() / 3600

		if utc_offset % 1 == 0:
			utc_offset = int(utc_offset)
			utc_offset_str = f'+{utc_offset}' if utc_offset >= 0 else f'{utc_offset}'
		else:
			utc_offset_hours = math.floor(utc_offset)
			utc_offset_minutes = int((utc_offset % 1) * 60)
			utc_offset_str = f'{utc_offset_hours}:{utc_offset_minutes}'
			utc_offset_str = f'+{utc_offset_str}' if utc_offset >= 0 else f'{utc_offset_str}'

		tz_offset_dict[chat_row['chat']] = (utc_offset, utc_offset_str)

	return tz_offset_dict
