import os
import sys
import time
import datetime
import sqlite3
import logging
import inspect
import telegram

from apscheduler.schedulers.background import BackgroundScheduler
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from db import create_chats_db, update_stats_db
from timezone import load_bulk_tz_offset
from tools import (short_monospaced_text, map_country_code_to_flag,
	reconstruct_link_for_markdown, reconstruct_message_for_markdown,
	anonymize_id, suffixed_readable_int, timestamp_to_legible_date_string,
	retry_after, time_delta_to_legible_eta)


def postpone_notification(
		db_path: str, postpone_tuple: tuple, bot: 'telegram.bot.Bot'):

	def send_postpone_notification(chat_id: str, launch_id: str):

		try:
			keyboard = InlineKeyboardMarkup(inline_keyboard=[[
				InlineKeyboardButton(text='🔇 Mute this launch',
				callback_data=f'mute/{launch_id}/1')
			]])

			sent_msg = bot.sendMessage(chat_id,
				message,
				parse_mode='MarkdownV2',
				reply_markup=keyboard)

			msg_identifier = f'{sent_msg["chat"]["id"]}:{sent_msg["message_id"]}'
			return True, msg_identifier

		except telegram.error.RetryAfter as error:
			retry_time = error.retry_after
			retry_after(retry_time)

			return False, None

		except telegram.error.TimedOut as error:
			logging.exception(
				'telegram.error.TimedOut: ждем секунду')
			retry_after(1)

			return False, None

		except telegram.error.Unauthorized as error:
			logging.info(f'{error}')

			clean_chats_db(db_path, chat_id)

			return True, None

		except telegram.error.ChatMigrated as error:
			conn = sqlite3.connect(os.path.join(db_path, 'launchbot-data.db'))
			cursor = conn.cursor()

			try:
				cursor.execute('UPDATE chats SET chat = ? WHERE chat = ?',
					(error.new_chat_id, chat_id))
			except:
				pass
			conn.commit()
			conn.close()
			clean_chats_db(db_path, chat_id)
			return True, None

		else:
			return True, None

	launch_obj = postpone_tuple[0]
	postpone_msg = postpone_tuple[1]

	old_notif_states = postpone_tuple[2]

	if len(launch_obj.lsp_name) > len('Virgin Orbit'):
		lsp_db_name = launch_obj.lsp_short
	else:
		lsp_db_name = launch_obj.lsp_name

	notification_list = get_notify_list(db_path=db_path,
		lsp=lsp_db_name,
		launch_id=launch_obj.unique_id,
		notify_class='postpone',
		notif_states=old_notif_states)

	notification_list_tzs = load_bulk_tz_offset(data_dir=db_path,
		chat_id_set=notification_list)

	API_SEND_LIMIT_PER_SECOND = 4
	messages_sent = 0
	send_start_time = int(time.time())

	sent_notification_ids = set()
	for chat, tz_tuple in notification_list_tzs.items():
		utc_offset = 3600 * tz_tuple[0]
		launch_unix = datetime.datetime.utcfromtimestamp(launch_obj.net_unix +
			utc_offset)

		if launch_unix.minute < 10:
			launch_time = f'{launch_unix.hour}:0{launch_unix.minute}'
		else:
			launch_time = f'{launch_unix.hour}:{launch_unix.minute}'

		time_string = f'`{launch_time}` `UTC{tz_tuple[1]}`'
		message = postpone_msg.replace('LAUNCHTIMEHERE', time_string)

		date_string = timestamp_to_legible_date_string(
			timestamp=launch_obj.net_unix + utc_offset, use_utc=True)

		message = message.replace('DATEHERE', date_string)

		success, msg_id = send_postpone_notification(chat_id=chat,
			launch_id=launch_obj.unique_id)

		if success and msg_id is not None:
			sent_notification_ids.add(msg_id)
		elif not success:

			fail_count = 0
			while fail_count < 5:
				fail_count += 1
				success, msg_id = send_postpone_notification(chat_id=chat,
					launch_id=launch_obj.unique_id)

				if success:
					break

				time.sleep(1)

			if success and msg_id is not None:
				sent_notification_ids.add(msg_id)
		time.sleep(1 / API_SEND_LIMIT_PER_SECOND)

		messages_sent += 1
		if messages_sent % 50 == 0:
			time.sleep(3)

	send_end_time = int(time.time())
	eta_string = time_delta_to_legible_eta(send_end_time - send_start_time,
		True)

	return notification_list, sent_notification_ids


def get_user_notifications_status(db_dir: str, chat: str, provider_set: set,
	provider_name_map: dict):
	conn = sqlite3.connect(os.path.join(db_dir, 'launchbot-data.db'))
	conn.row_factory = sqlite3.Row
	cursor = conn.cursor()

	cursor.execute(
		'SELECT name FROM sqlite_master WHERE type = ? AND name = ?',
		('table', 'chats'))
	if len(cursor.fetchall()) == 0:
		create_chats_db(db_path=db_dir, cursor=cursor)
		conn.commit()

	cursor.execute("SELECT * FROM chats WHERE chat = ?", (chat, ))
	query_return = [dict(row) for row in cursor.fetchall()]
	conn.close()

	notification_statuses = {'All': 0}
	mapped_provider_set = set()

	for provider in provider_set:
		if provider in provider_name_map.keys():
			provider = provider_name_map[provider]
		notification_statuses[provider] = 0

		mapped_provider_set.add(provider)

	provider_set = mapped_provider_set

	if len(query_return) == 0:
		return notification_statuses

	all_flag = False

	chat_row = query_return[0]

	if chat_row['enabled_notifications'] is not None:
		enabled_notifs = chat_row['enabled_notifications'].split(',')
	else:
		enabled_notifs = []

	if chat_row['disabled_notifications'] is not None:
		disabled_notifs = chat_row['disabled_notifications'].split(',')
	else:
		disabled_notifs = []

	for enabled_lsp in enabled_notifs:
		if enabled_lsp != '':
			if enabled_lsp in provider_set:
				notification_statuses[enabled_lsp] = 1

			if enabled_lsp == 'All':
				all_flag = True

	for disabled_lsp in disabled_notifs:
		if disabled_lsp != '':
			if disabled_lsp in provider_set:
				notification_statuses[disabled_lsp] = 0

			if disabled_lsp == 'All':
				all_flag = False

	notification_statuses['All'] = {True: 1, False: 0}[all_flag]
	return notification_statuses


def store_notification_identifiers(
		db_path: str, launch_id: str, identifiers: str):
	conn = sqlite3.connect(os.path.join(db_path, 'launchbot-data.db'))
	cursor = conn.cursor()

	update_tuple = (identifiers, launch_id)

	try:
		cursor.execute(
			'UPDATE launches SET sent_notification_ids = ? WHERE unique_id = ?',
			update_tuple)
	except:
		pass
	conn.commit()
	conn.close()


def toggle_notification(data_dir: str, chat: str, toggle_type: str,
	keyword: str, toggle_to_state: int, provider_by_cc: dict,
	provider_name_map: dict):
	conn = sqlite3.connect(os.path.join(data_dir, 'launchbot-data.db'))
	conn.row_factory = sqlite3.Row
	cursor = conn.cursor()

	if toggle_type == 'country_code':
		provider_list = set(provider_by_cc[keyword])
		provider_list_mod = set()

		for key in provider_list:
			if key in provider_name_map.keys():
				provider_list_mod.add(provider_name_map[key])
			else:
				provider_list_mod.add(key)

		provider_list = provider_list_mod

	elif toggle_type == 'lsp':
		if keyword in provider_name_map.keys():
			keyword = provider_name_map[keyword]

		provider_list = {keyword}

	elif toggle_type == 'all':
		provider_list = {'All'}
		provider_list_mod = {'All'}

		for cc_list in provider_by_cc.values():
			for provider in cc_list:
				if provider in provider_name_map.keys():
					provider_list_mod.add(provider_name_map[provider])
				else:
					provider_list_mod.add(provider)

		provider_list = provider_list_mod
	cursor.execute('SELECT * FROM chats WHERE chat = ?', (chat, ))
	query_return = [dict(row) for row in cursor.fetchall()]
	data_exists = bool(len(query_return) != 0)

	if data_exists:
		if query_return[0]['enabled_notifications'] is not None:
			old_enabled_states = query_return[0][
				'enabled_notifications'].split(',')
		else:
			old_enabled_states = []

		if query_return[0]['disabled_notifications'] is not None:
			old_disabled_states = query_return[0][
				'disabled_notifications'].split(',')
		else:
			old_disabled_states = []

		try:
			old_enabled_states.remove('')
		except ValueError:
			pass

		try:
			old_disabled_states.remove('')
		except ValueError:
			pass

	old_states = {}
	if data_exists:
		for enabled in old_enabled_states:
			old_states[enabled] = 1

		for disabled in old_disabled_states:
			old_states[disabled] = 0

	new_states = old_states

	if toggle_type == 'lsp':
		if keyword in old_states:
			new_states[keyword] = 1 if old_states[keyword] == 0 else 0
		else:
			new_states[keyword] = 1

		new_status = new_states[keyword]

	elif toggle_type in ('all', 'country_code'):
		for provider in provider_list:
			new_states[provider] = toggle_to_state

	new_enabled_notifications = set()
	new_disabled_notifications = set()
	for notification, state in new_states.items():
		if state == 1:
			new_enabled_notifications.add(notification)
		else:
			new_disabled_notifications.add(notification)

	new_enabled_str = ','.join(new_enabled_notifications)
	new_disabled_str = ','.join(new_disabled_notifications)

	if len(new_enabled_str) > 0:
		if new_enabled_str[0] == ',':
			new_enabled_str = new_enabled_str[1::]

	if len(new_disabled_str) > 0:
		if new_disabled_str[0] == ',':
			new_disabled_str = new_disabled_str[1::]

	try:
		if data_exists:
			cursor.execute(
				'''UPDATE chats SET enabled_notifications = ?, disabled_notifications = ?
				WHERE chat = ?''', (new_enabled_str, new_disabled_str, chat))
		else:
			cursor.execute(
				'''INSERT INTO chats (chat, subscribed_since, time_zone, time_zone_str,
				command_permissions, postpone_notify, notify_time_pref, enabled_notifications, 
				disabled_notifications) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
				(chat, int(time.time()), None, None, None, None, '1,1,1,1',
				new_enabled_str, new_disabled_str))
	except sqlite3.IntegrityError:
		cursor.execute(
			'''UPDATE chats SET enabled_notifications = ?, disabled_notifications = ?
				WHERE chat = ?''', (new_enabled_str, new_disabled_str, chat))

	conn.commit()
	conn.close()

	if toggle_type == 'lsp':
		return new_status

	return toggle_to_state


def update_notif_preference(db_path: str, chat: str,
	notification_type: str):
	old_preferences = list(get_notif_preference(db_path, chat))

	update_index = {'24h': 0, '12h': 1, '1h': 2, '5m': 3}[notification_type]
	new_state = 1 if old_preferences[update_index] == 0 else 0

	old_preferences[update_index] = new_state
	new_preferences = ','.join(str(val) for val in old_preferences)

	conn = sqlite3.connect(os.path.join(db_path, 'launchbot-data.db'))
	cursor = conn.cursor()
	try:
		cursor.execute(
			'''INSERT INTO chats
			(chat, subscribed_since, time_zone, time_zone_str, command_permissions, postpone_notify,
			notify_time_pref, enabled_notifications, disabled_notifications) VALUES (?,?,?,?,?,?,?,?,?)''',
			(chat, int(
			time.time()), None, None, None, None, new_preferences, None, None))
	except sqlite3.IntegrityError:
		cursor.execute("UPDATE chats SET notify_time_pref = ? WHERE chat = ?",
			(new_preferences, chat))

	conn.commit()
	conn.close()

	toggle_state_text = 'включено (🔔)' if new_state == 1 else 'отключено (🔕)'

	return new_state


def get_notif_preference(db_path: str, chat: str):
	conn = sqlite3.connect(os.path.join(db_path, 'launchbot-data.db'))
	cursor = conn.cursor()

	cursor.execute("SELECT notify_time_pref FROM chats WHERE chat = ?",
		(chat, ))
	query_return = cursor.fetchall()
	conn.close()

	if len(query_return) == 0:
		return (1, 1, 1, 1)

	notif_preferences = query_return[0][0].split(',')

	return (int(notif_preferences[0]), int(notif_preferences[1]),
		int(notif_preferences[2]), int(notif_preferences[3]))


def toggle_launch_mute(db_path: str, chat: str, launch_id: str, toggle: int):
	conn = sqlite3.connect(os.path.join(db_path, 'launchbot-data.db'))
	conn.row_factory = sqlite3.Row
	cursor = conn.cursor()

	chat = str(chat)

	cursor.execute("SELECT muted_by FROM launches WHERE unique_id = ?",
		(launch_id, ))
	query_return = [dict(row) for row in cursor.fetchall()]

	if len(query_return) == 0:
		logging.warning(
			f'No launches found to mute with launch_id={launch_id}')
		return

	if query_return[0]['muted_by'] is not None:
		muted_by = query_return[0]['muted_by'].split(',')
	else:
		muted_by = []

	if chat in muted_by and toggle == 0:
		muted_by.remove(chat)

	elif chat not in muted_by and toggle == 1:
		muted_by.append(chat)
		return

	muted_by_str = ','.join(muted_by)

	if len(muted_by_str) == 0:
		muted_by_str = None

	cursor.execute('UPDATE launches SET muted_by = ? WHERE unique_id = ?',
		(muted_by_str, launch_id))

	conn.commit()
	conn.close()


def load_mute_status(db_path: str, launch_id: str):
	conn = sqlite3.connect(os.path.join(db_path, 'launchbot-data.db'))
	cursor = conn.cursor()

	cursor.execute("SELECT muted_by FROM launches WHERE unique_id = ?",
		(launch_id, ))
	query_return = cursor.fetchall()
	conn.close()

	if len(query_return) == 0:
		return ()

	if query_return[0][0] is not None:
		muted_by = query_return[0][0].split(',')
	else:
		return ()

	return tuple(muted_by)


def clean_chats_db(db_path, chat):
	conn = sqlite3.connect(os.path.join(db_path, 'launchbot-data.db'))
	cursor = conn.cursor()

	cursor.execute("DELETE FROM chats WHERE chat = ?", (chat, ))
	conn.commit()
	conn.close()


def remove_previous_notification(
		db_path: str, launch_id: str, notify_set: set,
		bot: 'telegram.bot.Bot'):
	conn = sqlite3.connect(os.path.join(db_path, 'launchbot-data.db'))
	cursor = conn.cursor()

	cursor.execute(
		'SELECT sent_notification_ids FROM launches WHERE unique_id = ?',
		(launch_id, ))
	query_return = cursor.fetchall()

	if len(query_return) == 0:
		return

	identifiers = query_return[0][0]
	if identifiers in (None, ''):
		return

	try:
		identifiers = identifiers.split(',')
	except:
		return

	API_SEND_LIMIT_PER_SECOND = 4
	success_count, muted_count = 0, 0
	for id_pair in identifiers:
		id_pair = id_pair.split(':')

		try:
			chat_id, msg_id = id_pair[0], id_pair[1]
			message_identifier = (chat_id, msg_id)
		except IndexError:
			return

		if chat_id in notify_set:
			try:
				success = bot.delete_message(chat_id, msg_id)
				if success:
					success_count += 1
			except telegram.error.BadRequest:
				pass
			except telegram.error.RetryAfter as error:
				retry_time = error.retry_after
				retry_after(retry_time)

			except telegram.error.Unauthorized as error:
				if 'bot was kicked from the supergroup chat' in error.message:
					clean_chats_db(db_path, chat_id)

		else:
			muted_count += 1

		time.sleep(1 / API_SEND_LIMIT_PER_SECOND)



def get_notify_list(db_path: str, lsp: str, launch_id: str, notify_class: str,
	notif_states: tuple):
	conn = sqlite3.connect(os.path.join(db_path, 'launchbot-data.db'))
	conn.row_factory = sqlite3.Row
	cursor = conn.cursor()
	try:
		cursor.execute(
			"""
			SELECT * FROM chats WHERE enabled_notifications LIKE '%'||?||'%' 
			OR enabled_notifications LIKE '%'||?||'%'""", (lsp, 'All'))
	except sqlite3.OperationalError:
		conn.close()
		return set()

	query_return = cursor.fetchall()

	if len(query_return) == 0:
		conn.close()
		return set()

	muted_by = load_mute_status(db_path, launch_id)

	notification_list = set()

	if notify_class == 'postpone':

		for enum, state in enumerate(notif_states):
			enabled = bool(int(state) == 1)
			if int(state) == 0:
				if enum == 0:
					min_recvd_notif_idx = 0
				else:
					min_recvd_notif_idx = enum - 1

				break

			if enum == 3 and int(state) != 0:
				min_recvd_notif_idx = 3

		for chat_row in query_return:
			if chat_row['chat'] in muted_by:
				continue

			if lsp in chat_row['disabled_notifications']:
				continue

			chat_notif_prefs = chat_row['notify_time_pref'].split(',')

			for notif_state in range(min_recvd_notif_idx, -1, -1):
				if chat_notif_prefs[notif_state] == '1':
					notification_list.add(chat_row['chat'])
					break

		return notification_list

	notify_index = {
		'notify_24h': 0,
		'notify_12h': 1,
		'notify_60min': 2,
		'notify_5min': 3
	}[notify_class]

	for chat_row in query_return:
		if chat_row['chat'] in muted_by:
			continue

		if lsp in chat_row['disabled_notifications']:
			continue

		chat_notif_prefs = chat_row['notify_time_pref'].split(',')

		if chat_notif_prefs[notify_index] == '1':
			notification_list.add(chat_row['chat'])

	conn.close()
	return notification_list


def send_notification(chat: str, message: str, launch_id: str,
	notif_class: str, bot: 'telegram.bot.Bot', tz_tuple: tuple, net_unix: int,
	db_path: str):
	silent = bool(notif_class not in ('notify_60min', 'notify_5min'))

	utc_offset = 3600 * float(tz_tuple[0])
	launch_unix = datetime.datetime.utcfromtimestamp(net_unix + utc_offset)

	if launch_unix.minute < 10:
		launch_time = f'{launch_unix.hour}:0{launch_unix.minute}'
	else:
		launch_time = f'{launch_unix.hour}:{launch_unix.minute}'

	time_string = f'`{launch_time}` `UTC{tz_tuple[1]}`'
	message = message.replace('LAUNCHTIMEHERE', time_string)

	try:
		keyboard = InlineKeyboardMarkup(inline_keyboard=[[
			InlineKeyboardButton(text='🔇 Mute this launch',
			callback_data=f'mute/{launch_id}/1')
		]])

		sent_msg = bot.sendMessage(chat,
			message,
			parse_mode='MarkdownV2',
			reply_markup=keyboard,
			disable_notification=silent)

		msg_identifier = f'{sent_msg["chat"]["id"]}:{sent_msg["message_id"]}'
		return True, msg_identifier

	except telegram.error.RetryAfter as error:
		retry_time = error.retry_after
		retry_after(retry_time)

		return False, None

	except telegram.error.TimedOut as error:
		retry_after(1)

		return False, None

	except telegram.error.Unauthorized as error:
		clean_chats_db(db_path, chat)
		return True, None

	except telegram.error.ChatMigrated as error:
		conn = sqlite3.connect(os.path.join(db_path, 'launchbot-data.db'))
		cursor = conn.cursor()

		try:
			cursor.execute('UPDATE chats SET chat = ? WHERE chat = ?',
				(error.new_chat_id, chat))
		except:
			pass
		conn.commit()
		conn.close()

	except telegram.error.BadRequest as error:
		return True, None
		clean_chats_db(db_path, chat)
		return True, None

	else:
		return True, None


def create_notification_message(launch: dict, notif_class: str,
	bot_username: str):
	launch_name = launch['name'].split('|')[1].strip()

	provider_name_map = {
		'Rocket Lab Ltd': 'Rocket Lab',
		'Northrop Grumman Innovation Systems': 'Northrop Grumman',
		'Russian Federal Space Agency (ROSCOSMOS)': 'ROSCOSMOS'
	}

	if launch['lsp_name'] in provider_name_map.keys():
		lsp_name = provider_name_map[launch['lsp_name']]
	else:
		if len(launch['lsp_name']) > len('Galactic Energy'):
			if launch['lsp_short'] not in (None, ''):
				lsp_name = launch['lsp_short']
			else:
				lsp_name = launch['lsp_name']
		else:
			lsp_name = launch['lsp_name']

	lsp_flag = map_country_code_to_flag(launch['lsp_country_code'])

	if 'LC-' not in launch['pad_name']:
		launch['pad_name'] = launch['pad_name'].replace(
			'Space Launch Complex ', 'SLC-')
		launch['pad_name'] = launch['pad_name'].replace(
			'Launch Complex ', 'LC-')

	if 'air launch' in launch['pad_name'].lower():
		launch['pad_name'] = 'Air launch to orbit'

	launch_site = launch['location_name'].split(',')[0].strip()
	location_flag = map_country_code_to_flag(launch['location_country_code'])

	if 'Starship' in launch['rocket_name']:
		location = f'SpaceX South Texas Launch Site, Boca Chica {location_flag}'
	else:
		location = f'{launch["pad_name"]}, {launch_site} {location_flag}'

	mission_type = launch['mission_type'].capitalize(
	) if launch['mission_type'] is not None else 'Unknown purpose'

	orbit_map = {
		'Sub Orbital': 'Sub-orbital',
		'VLEO': 'Very low-Earth orbit',
		'LEO': 'Low-Earth orbit',
		'SSO': 'Sun-synchronous orbit',
		'PO': 'Polar orbit',
		'MEO': 'Medium-Earth orbit',
		'GEO': 'Geostationary (direct)',
		'GTO': 'Geostationary (transfer)',
		'GSO': 'Geosynchronous orbit',
		'LO': 'Lunar orbit'
	}

	try:
		orbit_info = '🌒' if 'LO' in launch['mission_orbit_abbrev'] else '🌍'
		if launch['mission_orbit_abbrev'] in orbit_map.keys():
			orbit_str = orbit_map[launch['mission_orbit_abbrev']]
		else:
			orbit_str = launch['mission_orbit'] if launch[
				'mission_orbit_abbrev'] is not None else 'Unknown'
			if 'Starlink' in launch_name:
				orbit_str = 'Very-low Earth orbit'
	except TypeError:
		orbit_info = '🌍'
		orbit_str = 'Unknown orbit'

	probability_map = {80: '☀️', 60: '🌤', 40: '🌥', 20: '☁️', 00: '⛈'}
	if launch['probability'] not in (-1, None):
		for prob_range_start, prob_str in probability_map.items():
			if launch['probability'] >= prob_range_start:
				probability = f"{prob_str} *{int(launch['probability'])} %* probability of launch"
	else:
		probability = None

	if launch['spacecraft_crew_count'] not in (None, 0):
		if 'Dragon' in launch['spacecraft_name']:
			spacecraft_info = True
		else:
			spacecraft_info = None
	else:
		spacecraft_info = None

	if isinstance(launch['launcher_landing_attempt'], str):
		multiple_boosters = bool(';;' in launch['launcher_landing_attempt'])
	else:
		multiple_boosters = False

	landing_loc_map = {
		'OCISLY': 'Atlantic Ocean',
		'JRTI': 'Atlantic Ocean',
		'ASLOG': 'Pacific Ocean',
		'LZ-1': 'CCAFS RTLS',
		'LZ-2': 'CCAFS RTLS',
		'LZ-4': 'VAFB RTLS',
		'ATL': 'Expend 💥',
		'PAC': 'Expend 💥'
	}

	if launch['launcher_landing_attempt'] and not multiple_boosters:
		core_str = launch['launcher_serial_number']
		core_str = 'Unknown' if core_str is None else core_str

		if launch['launcher_is_flight_proven']:
			reuse_count = launch['launcher_stage_flight_number']

			if lsp_name == 'SpaceX' and core_str[0:2] == 'B1':
				core_str += f'.{int(reuse_count)}'

			reuse_str = f'{core_str} ({suffixed_readable_int(reuse_count)} полет ♻️)'
		else:
			if lsp_name == 'SpaceX' and core_str[0:2] == 'B1':
				core_str += '.1'

			reuse_str = f'первый полет {core_str}'
		if launch['launcher_landing_location'] in landing_loc_map.keys():
			landing_type = landing_loc_map[launch['launcher_landing_location']]
			if launch['launcher_landing_location'] in ('ATL', 'PAC'):
				launch['launcher_landing_location'] = 'Ocean'

			landing_str = f"{launch['launcher_landing_location']} ({landing_type})"
		else:
			landing_type = launch['landing_type']
			if launch['launcher_landing_location'] in ('ATL', 'PAC'):
				launch['launcher_landing_location'] = 'Ocean'

			landing_str = f"{launch['launcher_landing_location']} ({landing_type})"

		if lsp_name == 'SpaceX' and 'Starship' in launch["rocket_name"]:
			location = f'SpaceX South Texas Launch Site, Boca Chica {location_flag}'

			recovery_str = '*Vehicle information* 🚀'
			recovery_str += f'\n\t*Starship* {short_monospaced_text(reuse_str)}'
			recovery_str += f'\n\t*Landing* {short_monospaced_text(landing_str)}'

		else:
			recovery_str = '*Vehicle information* 🚀'
			recovery_str += f'\n\t*Core* {short_monospaced_text(reuse_str)}'
			recovery_str += f'\n\t*Landing* {short_monospaced_text(landing_str)}'

	elif multiple_boosters:
		booster_indices = {'core': None, 'boosters': []}
		for enum, stage_type in enumerate(
			launch['launcher_stage_type'].split(';;')):
			if stage_type.lower() == 'core':
				booster_indices['core'] = enum
			elif stage_type.lower() == 'strap-on booster':
				booster_indices['boosters'].append(enum)

		recovery_str = '''Конфигурация ракеты'''

		indices = [booster_indices['core']] + booster_indices['boosters']
		for enum, idx in enumerate(indices):
			is_core = bool(enum == 0)

			core_str = launch['launcher_serial_number'].split(';;')[idx]
			core_str = 'Unknown' if core_str is None else core_str

			if launch['launcher_is_flight_proven'].split(';;')[idx]:
				reuse_count = launch['launcher_stage_flight_number'].split(
					';;')[idx]

				if lsp_name == 'SpaceX' and core_str[0:2] == 'B1':
					core_str += f'.{int(reuse_count)}'

				reuse_str = f'{core_str} ({suffixed_readable_int(int(reuse_count))} полет ♻️)'
			else:
				if lsp_name == 'SpaceX' and core_str[0:2] == 'B1':
					core_str += '.1'

				reuse_str = f'первый полет {core_str}'

			landing_loc = launch['launcher_landing_location'].split(';;')[idx]
			if landing_loc in landing_loc_map.keys():
				landing_type = landing_loc_map[landing_loc]
				if landing_loc in ('ATL', 'PAC'):
					landing_loc = 'Ocean'

				landing_str = f"{landing_loc} ({landing_type})"
			else:
				landing_type = launch['landing_type'].split(';;')[idx]
				landing_str = f"{landing_loc} ({landing_type})"

			if is_core:
				booster_str = f'\n\t*Core* {short_monospaced_text(reuse_str)}'
				booster_str += f'\n\t*↪* {short_monospaced_text(landing_str)}'
			else:
				booster_str = f'*\n\tBooster* {short_monospaced_text(reuse_str)}'
				booster_str += f'\n\t*↪* {short_monospaced_text(landing_str)}'

			recovery_str += booster_str

	else:
		recovery_str = None

	if launch['mission_description'] not in ('', None):
		if launch['mission_description'] is None:
			info_str = 'No launch information available.'
		else:
			info_str = launch['mission_description']

		info_text = f'ℹ️ {info_str}'
	else:
		info_text = None

	if notif_class in ('notify_60min', 'notify_5min'):
		vid_url = None
		try:
			urls = launch['webcast_url_list'].split(',')
		except AttributeError:
			urls = set()

		if len(urls) == 0:
			link_text = '*нет видео запуска*'
		else:
			for url in urls:
				if 'youtube' in url:
					vid_url = url
					break

			if vid_url is None:
				vid_url = urls[0]

			link_text = '🔴 *смотрите онлайн* LinkTextGoesHere'
	else:
		link_text = None

	t_minus = {
		'notify_24h': '24 часа',
		'notify_12h': '12 часа',
		'notify_60min': '60 минур',
		'notify_5min': '5 минут'
	}

	base_message = f'''
	*{launch_name}* начнет запуск *{t_minus[notif_class]}*
	*Спонсор запуска* {short_monospaced_text(lsp_name)}
	*Ракета* {short_monospaced_text(launch["rocket_name"])}
	*Находится в * {short_monospaced_text(location)}

	*Информация* {orbit_info}
	*Тип* {short_monospaced_text(mission_type)}
	*На какой орбите* {short_monospaced_text(orbit_str)}
	'''

	if spacecraft_info is not None:
		base_message += '\n\t'
		base_message += '*Dragon information* 🐉\n\t'
		base_message += f'*Crew* {short_monospaced_text("👨‍🚀" * launch["spacecraft_crew_count"])}\n\t'
		base_message += f'*Capsule* {short_monospaced_text(launch["spacecraft_sn"])}'
		base_message += '\n\t'

	if recovery_str is not None:
		base_message += '\n\t'
		base_message += recovery_str
		base_message += '\n\t'

	if info_text is not None:
		base_message += '\n\t'
		base_message += info_text
		base_message += '\n\t'

	if link_text is not None:
		base_message += '\n\t'
		base_message += link_text

	footer = f'''
	🕓 *Время до запуска* LAUNCHTIMEHERE
	🔕 *чтобы замутить* /notify@{bot_username}'''
	base_message += footer

	base_message = reconstruct_message_for_markdown(base_message)

	if link_text is not None and 'LinkTextGoesHere' in base_message:
		base_message = base_message.replace(
			'LinkTextGoesHere',
			f'[live\!]({reconstruct_link_for_markdown(vid_url)})')

	return inspect.cleandoc(base_message)


def notification_handler(db_path: str, notification_dict: dict,
	bot_username: str, bot: 'telegram.bot.Bot'):

	def verify_launch_is_up_to_date(launch_uid: str, cursor: sqlite3.Cursor):
		cursor.execute('SELECT last_updated FROM launches WHERE unique_id = ?',
			(launch_uid, ))
		query_return = cursor.fetchall()

		if len(query_return) == 0:
			logging.warning(
				f'verify_launch_is_up_to_date couldn\'t find launch with id={launch_uid}'
			)
			return False

		launch_last_update = query_return[0][0]

		cursor.execute('SELECT last_api_update FROM stats')

		try:
			last_api_update = cursor.fetchall()[0][0]
		except KeyError:
			return False

		if launch_last_update == last_api_update:
			return True
		cursor.execute('DELETE FROM launches WHERE unique_id = ?',
			(launch_uid, ))
		return False

	conn = sqlite3.connect(os.path.join(db_path, 'launchbot-data.db'))
	conn.row_factory = sqlite3.Row
	cursor = conn.cursor()

	for launch_id, notify_class in notification_dict.items():
		cursor.execute("SELECT * FROM launches WHERE unique_id = ?",
			(launch_id, ))
		launch_dict = [dict(row) for row in cursor.fetchall()][0]

		launch_id = launch_dict['unique_id']

		cursor.execute(
			f"UPDATE launches SET {notify_class} = 1 WHERE unique_id = ?",
			(launch_id, ))
		conn.commit()
		up_to_date = verify_launch_is_up_to_date(launch_uid=launch_id,
			cursor=cursor)

		if not up_to_date:
			conn.commit()
			conn.close()
			return

		notification_message = create_notification_message(launch=launch_dict,
			notif_class=notify_class,
			bot_username=bot_username)

		logging.info(notification_message)

		if len(launch_dict['lsp_name']) > len('Virgin Orbit'):
			lsp_db_name = launch_dict['lsp_short']
		else:
			lsp_db_name = launch_dict['lsp_name']


		notification_list = get_notify_list(db_path=db_path,
			lsp=lsp_db_name,
			launch_id=launch_id,
			notify_class=notify_class,
			notif_states=None)


		notification_list_tzs = load_bulk_tz_offset(data_dir=db_path,
			chat_id_set=notification_list)

		without_sound = bool(notify_class not in ('notify_60min',
			'notify_5min'))

		API_SEND_LIMIT_PER_SECOND = 4

		messages_sent = 0
		send_start_time = int(time.time())

		approx_send_time = 1 / API_SEND_LIMIT_PER_SECOND * len(
			notification_list_tzs)
		send_delta = time_delta_to_legible_eta(int(approx_send_time), True)

		sent_notification_ids = set()
		for chat_id, tz_tuple in notification_list_tzs.items():
			try:
				success, msg_id = send_notification(chat=chat_id,
					message=notification_message,
					launch_id=launch_id,
					notif_class=notify_class,
					bot=bot,
					tz_tuple=tz_tuple,
					net_unix=launch_dict['net_unix'],
					db_path=db_path)
			except Exception as error:
				continue

			if success and msg_id is not None:
				sent_notification_ids.add(msg_id)
			elif not success:
				fail_count = 0
				while fail_count < 5:
					fail_count += 1
					success, msg_id = send_notification(chat=chat_id,
						message=notification_message,
						launch_id=launch_id,
						notif_class=notify_class,
						bot=bot,
						tz_tuple=tz_tuple,
						net_unix=launch_dict['net_unix'],
						db_path=db_path)

					if success:
						break

					time.sleep(1)
				if success and msg_id is not None:
					sent_notification_ids.add(msg_id)
			time.sleep(1 / API_SEND_LIMIT_PER_SECOND)

			messages_sent += 1
			if messages_sent % 50 == 0:
				time.sleep(3)

		send_end_time = int(time.time())
		eta_string = time_delta_to_legible_eta(send_end_time - send_start_time,
			True)

		remove_previous_notification(db_path=db_path,
			launch_id=launch_id,
			notify_set=notification_list,
			bot=bot)

		msg_id_str = ','.join(sent_notification_ids)
		store_notification_identifiers(db_path=db_path,
			launch_id=launch_id,
			identifiers=msg_id_str)
		update_stats_db(stats_update={'notifications': len(notification_list)},
			db_path=db_path)

	conn.close()


def clear_missed_notifications(db_path: str, launch_id_dict_list: list):
	conn = sqlite3.connect(os.path.join(db_path, 'launchbot-data.db'))
	cursor = conn.cursor()
	miss_count = 0
	for launch_id_dict in launch_id_dict_list:
		for uid, missed_notification in launch_id_dict.items():
			cursor.execute(
				f'''UPDATE launches SET {missed_notification} = 1 WHERE unique_id = ?''',
				(uid, ))
			miss_count += 1
	conn.commit()
	conn.close()


def notification_send_scheduler(db_path: str, next_api_update_time: int,
	scheduler: BackgroundScheduler, bot_username: str,
	bot: 'telegram.bot.Bot'):
	conn = sqlite3.connect(os.path.join(db_path, 'launchbot-data.db'))
	cursor = conn.cursor()

	select_fields = 'net_unix, unique_id, status_state'
	select_fields += ', notify_24h, notify_12h, notify_60min, notify_5min'

	notify_window = int(time.time()) - 60 * 5

	try:
		cursor.execute(
			f'SELECT {select_fields} FROM launches WHERE net_unix >= ?',
			(notify_window, ))
		query_return = cursor.fetchall()
	except sqlite3.OperationalError:
		query_return = set()

	if len(query_return) == 0:
		return

	query_return.sort(key=lambda tup: tup[0])

	notif_send_times, time_map = {}, {
		0: 24 * 3600 + 5 * 60,
		1: 12 * 3600 + 5 * 60,
		2: 3600 + 5 * 60,
		3: 5 * 60 + 7 * 60
	}
	for launch_row in query_return:
		launch_status = launch_row[2]
		if launch_status == 'TBD':
			continue

		for enum, notif_bool in enumerate(launch_row[3::]):
			if not notif_bool:
				send_time = launch_row[0] - time_map[enum]

				uid = launch_row[1]

				notify_class_map = {
					0: 'notify_24h',
					1: 'notify_12h',
					2: 'notify_60min',
					3: 'notify_5min'
				}
				if send_time not in notif_send_times:
					notif_send_times[send_time] = {uid: notify_class_map[enum]}
				else:
					if uid not in notif_send_times:
						notif_send_times[send_time][uid] = notify_class_map[
							enum]
					else:
						notif_send_times[send_time][uid] = notify_class_map[
							enum]

	cleared_count = 0
	for job in scheduler.get_jobs():
		if 'notification' in job.id:
			scheduler.remove_job(job.id)
			cleared_count += 1

	scheduled_notifications, missed_notifications = 0, []
	for send_time, notification_dict in notif_send_times.items():
		if send_time > next_api_update_time:
			pass
		elif send_time < time.time() - 60 * 5:
			missed_notifications.append(notification_dict)
		else:
			if send_time < time.time():
				send_time_offset = int(time.time() - send_time)
				send_time = time.time() + 3

			notification_dt = datetime.datetime.fromtimestamp(send_time + 2)

			scheduler.add_job(notification_handler,
				'date',
				id=f'notification-{int(send_time)}',
				run_date=notification_dt,
				args=[db_path, notification_dict, bot_username, bot])
			scheduled_notifications += 1

	if len(missed_notifications) != 0:
		clear_missed_notifications(db_path, missed_notifications)
	conn.close()
