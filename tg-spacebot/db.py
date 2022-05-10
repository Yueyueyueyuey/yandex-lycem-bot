import os
import time
import sqlite3
import logging
import datetime
import inspect

import redis
import ujson as json

from tools import time_delta_to_legible_eta, reconstruct_message_for_markdown


def create_chats_db(db_path: str, cursor: sqlite3.Cursor):

	if not os.path.isdir(db_path):
		os.makedirs(db_path)

	try:
		cursor.execute('''
			CREATE TABLE chats (chat TEXT, subscribed_since INT, member_count INT,
			time_zone TEXT, time_zone_str TEXT, command_permissions TEXT, postpone_notify BOOLEAN,
			notify_time_pref TEXT, enabled_notifications TEXT, disabled_notifications TEXT,
			PRIMARY KEY (chat))
			''')

		cursor.execute(
			"CREATE INDEX chatenabled ON chats (chat, enabled_notifications)")
		cursor.execute(
			"CREATE INDEX chatdisabled ON chats (chat, disabled_notifications)"
		)
	except sqlite3.OperationalError as error:
		logging.exception(f'{error}')


def migrate_chat(db_path: str, old_id: int, new_id: int):
	conn = sqlite3.connect(os.path.join(db_path, 'launchbot-data.db'))
	cursor = conn.cursor()

	try:
		cursor.execute('UPDATE chats SET chat = ? WHERE chat = ?',
			(new_id, old_id))
	except:
		pass
	conn.commit()
	conn.close()


def create_launch_db(db_path: str, cursor: sqlite3.Cursor):

	try:
		cursor.execute('''CREATE TABLE launches
			(name TEXT, unique_id TEXT, ll_id INT, net_unix INT, status_id INT, status_state TEXT,
			in_hold BOOLEAN, probability REAL, success BOOLEAN, tbd_time BOOLEAN, tbd_date BOOLEAN,
			launched BOOLEAN,

			webcast_islive BOOLEAN, webcast_url_list TEXT,

			lsp_id INT, lsp_name TEXT, lsp_short TEXT, lsp_country_code TEXT,
			
			mission_name TEXT, mission_type TEXT, mission_orbit TEXT, mission_orbit_abbrev TEXT,
			mission_description TEXT,

			pad_name TEXT, location_name TEXT, location_country_code TEXT,

			rocket_name TEXT, rocket_full_name TEXT, rocket_variant TEXT, rocket_family TEXT,
			
			launcher_stage_id TEXT, launcher_stage_type TEXT, launcher_stage_is_reused BOOLEAN,
			launcher_stage_flight_number INT, launcher_stage_turn_around TEXT, launcher_is_flight_proven BOOLEAN,
			launcher_serial_number TEXT, launcher_maiden_flight INT, launcher_last_flight INT,
			launcher_landing_attempt BOOLEAN, launcher_landing_location TEXT, landing_type TEXT,
			launcher_landing_location_nth_landing INT,

			spacecraft_id INT, spacecraft_sn TEXT, spacecraft_name TEXT, spacecraft_crew TEXT,
			spacecraft_crew_count INT, spacecraft_maiden_flight INT,

			pad_nth_launch INT, location_nth_launch INT, agency_nth_launch INT, agency_nth_launch_year INT,
			orbital_nth_launch_year INT, 

			last_updated INT,

			notify_24h BOOLEAN, notify_12h BOOLEAN, notify_60min BOOLEAN, notify_5min BOOLEAN,

			muted_by TEXT, sent_notification_ids TEXT,
			PRIMARY KEY (unique_id))
		''')

		cursor.execute(
			"CREATE INDEX name_to_unique_id ON launches (name, unique_id)")
		cursor.execute(
			"CREATE INDEX unique_id_to_lsp_short ON launches (unique_id, lsp_short)"
		)
		cursor.execute(
			"CREATE INDEX net_unix_to_lsp_short ON launches (net_unix, lsp_short)"
		)

	except sqlite3.OperationalError as e:
		pass

def update_launch_db(
		launch_set: set, db_path: str, bot_username: str, api_update: int):

	def verify_no_net_slip(launch_object: 'LaunchLibrary2Launch',
		cursor: sqlite3.Cursor) -> (bool, tuple):

		cursor.execute('SELECT * FROM launches WHERE unique_id = ?',
			(launch_object.unique_id, ))
		query_return = [dict(row) for row in cursor.fetchall()]
		launch_db = query_return[0]

		if launch_db['net_unix'] == launch_object.net_unix:
			return (False, ())

		net_diff = launch_object.net_unix - launch_db['net_unix']

		notification_states = {
			'notify_24h': launch_db['notify_24h'],
			'notify_12h': launch_db['notify_12h'],
			'notify_60min': launch_db['notify_60min'],
			'notify_5min': launch_db['notify_5min']
		}

		old_notification_states = tuple(notification_states.values())

		notif_pre_time_map = {
			'notify_24h': 24,
			'notify_12h': 12,
			'notify_60min': 1,
			'notify_5min': 5 / 60
		}

		notification_state_reset = False
		skipped_postpones = []

		if 1 in notification_states.values(
		) and net_diff >= 5 * 60 and not launch_object.launched:
			for key, status in notification_states.items():
				until_launch = launch_object.net_unix - int(time.time())
				window_end = launch_db[
					'net_unix'] - 3600 * notif_pre_time_map[key]
				window_diff = window_end - int(time.time()) + net_diff

				if int(
					status) == 1 and int(time.time()) - net_diff < window_end:
					postpone = {
						'old net': launch_db['net_unix'],
						'launch_obj.net_unix': launch_object.net_unix,
						'time.time() - net_diff': int(time.time()) - net_diff,
						'window_end': window_end,
						'until_launch': until_launch,
						'window_diff': window_diff
					}

					notification_states[key] = 0
					notification_state_reset = True
				else:
					postpone = {
						'status': status,
						'net_diff': net_diff,
						'multipl.': notif_pre_time_map[key],
						'time.time() + net_diff': int(time.time()) + net_diff,
						'window_end': window_end,
						'until_launch': until_launch,
						'window_diff': window_diff
					}

					skipped_postpones.append(postpone)

			if not notification_state_reset:
				return (False, ())


			postpone_str = time_delta_to_legible_eta(time_delta=int(net_diff),
				full_accuracy=False)

			eta_sec = launch_object.net_unix - time.time()
			next_attempt_eta_str = time_delta_to_legible_eta(
				time_delta=int(eta_sec), full_accuracy=False)

			try:
				launch_name = launch_object.name.split('|')[1].strip()
			except IndexError:
				launch_name = launch_object.name.strip()

			postpone_msg = f'📢 *{launch_name}* перенесен на  {postpone_str}. '
			postpone_msg += f'*{launch_object.lsp_name}* взлетит *DATEHERE* в *LAUNCHTIMEHERE*.'
			postpone_msg += f'\n\n⏱ {next_attempt_eta_str} до следующего запуска'

			postpone_msg = reconstruct_message_for_markdown(postpone_msg)

			postpone_msg += '\n\nвы будете подключены к этому запуску\. '
			postpone_msg += f'для дполнительной информации используйте \/next\@{bot_username}\. '
			postpone_msg += 'чтобы убрать, используйте кнопку ниже\._'

			postpone_msg = inspect.cleandoc(postpone_msg)

			insert_statement = '=?,'.join(notification_states.keys()) + '=?'

			values_tuple = tuple(
				notification_states.values()) + (launch_object.unique_id, )

			cursor.execute(
				f'UPDATE launches SET {insert_statement} WHERE unique_id = ?',
				values_tuple)

			postpone_tup = (launch_object, postpone_msg,
				old_notification_states)

			return (True, postpone_tup)

		return (False, ())

	if not os.path.isfile(os.path.join(db_path, 'launchbot-data.db')):
		if not os.path.isdir(db_path):
			os.makedirs(db_path)

	conn = sqlite3.connect(os.path.join(db_path, 'launchbot-data.db'))
	conn.row_factory = sqlite3.Row
	cursor = conn.cursor()

	cursor.execute(
		'SELECT name FROM sqlite_master WHERE type = ? AND name = ?',
		('table', 'launches'))
	if len(cursor.fetchall()) == 0:
		create_launch_db(db_path=db_path, cursor=cursor)

	slipped_launches = set()
	for launch_object in launch_set:
		try:
			insert_fields = ', '.join(vars(launch_object).keys())
			insert_fields += ', last_updated, notify_24h, notify_12h, notify_60min, notify_5min'
			field_values = tuple(vars(launch_object).values()) + (api_update,
				False, False, False, False)

			values_string = '?,' * (len(vars(launch_object).keys()) + 5)
			values_string = values_string[0:-1]

			cursor.execute(
				f'INSERT INTO launches ({insert_fields}) VALUES ({values_string})',
				field_values)

		except sqlite3.IntegrityError: 
			obj_dict = vars(launch_object)

			update_fields = obj_dict.keys()
			update_values = obj_dict.values()

			set_str = ' = ?, '.join(update_fields) + ' = ?'

			net_slipped, postpone_tuple = verify_no_net_slip(
				launch_object=launch_object, cursor=cursor)

			if net_slipped:
				slipped_launches.add(postpone_tuple)

			try:
				cursor.execute(
					f"UPDATE launches SET {set_str} WHERE unique_id = ?",
					tuple(update_values) + (launch_object.unique_id, ))
				cursor.execute(
					"UPDATE launches SET last_updated = ? WHERE unique_id = ?",
					(api_update, ) + (launch_object.unique_id, ))
			except Exception:
				logging.exception(
					f'⚠️ Error updating field for unique_id={launch_object.unique_id}!'
				)

	conn.commit()
	conn.close()

	if len(slipped_launches) > 0:
		return slipped_launches

	return set()


def create_stats_db(db_path: str):
	if not os.path.isdir(db_path):
		os.mkdir(db_path)

	conn = sqlite3.connect(os.path.join(db_path, 'launchbot-data.db'))
	cursor = conn.cursor()

	try:
		cursor.execute('''CREATE TABLE stats 
			(notifications INT, api_requests INT, db_updates INT, commands INT,
			data INT, last_api_update INT, PRIMARY KEY (notifications, api_requests))'''
						)

		cursor.execute('''INSERT INTO stats 
			(notifications, api_requests, db_updates, commands, data, last_api_update)
			VALUES (0, 0, 0, 0, 0, 0)''')
	except sqlite3.OperationalError as sqlite_error:
		logging.warn('ошибка создания бд %s', sqlite_error)

	conn.commit()
	conn.close()


def update_stats_db(stats_update: dict, db_path: str):
	if not os.path.isfile(os.path.join(db_path, 'launchbot-data.db')):
		create_stats_db(db_path=db_path)

	stats_conn = sqlite3.connect(os.path.join(db_path, 'launchbot-data.db'))
	stats_cursor = stats_conn.cursor()
	rd = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

	stats_cursor.execute(
		'SELECT name FROM sqlite_master WHERE type = ? AND name = ?',
		('table', 'stats'))
	if len(stats_cursor.fetchall()) == 0:
		create_stats_db(db_path)

	if not rd.exists('stats'):
		stats_conn = sqlite3.connect(os.path.join(db_path,
			'launchbot-data.db'))
		stats_conn.row_factory = sqlite3.Row
		stats_cursor = stats_conn.cursor()

		try:
			stats_cursor.execute("SELECT * FROM stats")
			stats = [dict(row) for row in stats_cursor.fetchall()][0]
		except sqlite3.OperationalError:
			stats = {
				'notifications': 0,
				'api_requests': 0,
				'db_updates': 0,
				'commands': 0,
				'data': 0,
				'last_api_update': 0
			}

		if stats['last_api_update'] is None:
			stats['last_api_update'] = int(time.time())

		rd.hmset('stats', stats)

	for stat, val in stats_update.items():
		if stat == 'last_api_update':
			stats_cursor.execute(f"UPDATE stats SET {stat} = {val}")
			rd.hset('stats', stat, val)
		else:
			stats_cursor.execute(f"UPDATE stats SET {stat} = {stat} + {val}")
			try:
				rd.hset('stats', stat, int(rd.hget('stats', stat)) + int(val))
			except TypeError:
				logging.exception(
					f'ошибка обновления бд репозитория'
				)

				stats_conn.close()
				return

	stats_conn.commit()
	stats_conn.close()
