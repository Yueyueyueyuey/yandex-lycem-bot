import os
import sys
import time
import logging
import difflib
import datetime
import sqlite3

import redis
import requests
import coloredlogs
import ujson as json

from requests.adapters import HTTPAdapter
from apscheduler.schedulers.background import BackgroundScheduler
from nltk import tokenize

from tools import timestamp_to_unix, time_delta_to_legible_eta
from db import update_launch_db, update_stats_db
from notifications import (notification_send_scheduler, postpone_notification,
	remove_previous_notification, store_notification_identifiers)


class LaunchLibrary2Launch:
	def __init__(self, launch_json: dict):
		self.name = launch_json['name']
		self.unique_id = launch_json['id']
		self.ll_id = launch_json['launch_library_id']

		self.net_unix = timestamp_to_unix(launch_json['net'])
		self.status_id = launch_json['status']['id']
		self.status_state = launch_json['status']['abbrev']

		status_map = {
			'Go': 'GO',
			'Hold': 'HOLD',
			'In Flight': 'FLYING',
			'Success': 'SUCCESS',
			'Partial Failure': 'PFAILURE',
			'Failure': 'FAILURE'
		}

		if self.status_state in status_map.keys():
			self.status_state = status_map[self.status_state]

		self.in_hold = launch_json['inhold']
		self.success = bool('Success' in launch_json['status']['name'])

		self.probability = launch_json['probability']

		self.tbd_time = launch_json[
			'tbdtime'] if 'tbdtime' in launch_json else True
		self.tbd_date = launch_json[
			'tbddate'] if 'tbddate' in launch_json else True

		launch_bool = [
			status for status in ('success', 'failure')
			if status in self.status_state.lower()
		]
		self.launched = bool(any(launch_bool))

		try:
			self.lsp_id = launch_json['launch_service_provider']['id']
			self.lsp_name = launch_json['launch_service_provider']['name']
			self.lsp_short = launch_json['launch_service_provider']['abbrev']
			self.lsp_country_code = launch_json['launch_service_provider'][
				'country_code']
		except TypeError:
			self.lsp_id = None
			self.lsp_name = None
			self.lsp_short = None
			self.lsp_country_code = None
			logging.exception(
				f'{launch_json}'
			)

		self.webcast_islive = launch_json['webcast_live']
		self.webcast_url_list = None  

		if len(launch_json['vidURLs']) >= 1:
			priority_map = {}
			for url_dict in launch_json['vidURLs']:
				priority = url_dict['priority']
				url = url_dict['url']

				if priority in priority_map.keys():
					priority_map[priority] = priority_map[priority] + ',' + url
				else:
					priority_map[priority] = url

			try:
				highest_prior = min(priority_map.keys())
			except ValueError:
				highest_prior = None

			if highest_prior is not None:
				self.webcast_url_list = priority_map[highest_prior]
			else:
				logging.warning(
					f'ID: {self.unique_id}'
				)
				self.webcast_url_list = None
		else:
			self.webcast_url_list = None

		self.rocket_name = launch_json['rocket']['configuration']['name']
		self.rocket_full_name = launch_json['rocket']['configuration'][
			'full_name']
		self.rocket_variant = launch_json['rocket']['configuration']['variant']
		self.rocket_family = launch_json['rocket']['configuration']['family']

		if launch_json['rocket']['launcher_stage'] not in (None, []):
			stage_count = len(launch_json['rocket']['launcher_stage'])
		else:
			stage_count = 0

		if stage_count > 1:
			stages = launch_json['rocket']['launcher_stage']

			self.launcher_stage_id = ';;'.join(
				[str(stage['id']) for stage in stages])
			self.launcher_stage_type = ';;'.join(
				[str(stage['type']) for stage in stages])
			self.launcher_stage_is_reused = ';;'.join(
				[str(stage['reused']) for stage in stages])
			self.launcher_stage_flight_number = ';;'.join(
				[str(stage['launcher_flight_number']) for stage in stages])
			self.launcher_stage_turn_around = ';;'.join(
				[str(stage['turn_around_time_days']) for stage in stages])
			self.launcher_is_flight_proven = ';;'.join(
				[str(stage['launcher']['flight_proven']) for stage in stages])
			self.launcher_serial_number = ';;'.join(
				[str(stage['launcher']['serial_number']) for stage in stages])

			maiden_flights, last_flights = [], []
			landing_attempts, landing_locs, landing_types, landing_loc_nths = [], [], [], []
			for stage in stages:
				try:
					maiden_flight = timestamp_to_unix(
						stage['launcher']['first_launch_date'])
					last_flight = timestamp_to_unix(
						stage['launcher']['last_launch_date'])
				except:
					maiden_flight = None
					last_flight = None

				maiden_flights.append(str(maiden_flight))
				last_flights.append(str(last_flight))

				if stage['landing'] is not None:
					landing_json = stage['landing']
					landing_attempts.append(str(landing_json['attempt']))
					landing_locs.append(landing_json['location']['abbrev'])
					landing_types.append(landing_json['type']['abbrev'])
					landing_loc_nths.append(
						str(landing_json['location']['successful_landings']))
				else:
					landing_attempts.append(None)
					landing_locs.append(None)
					landing_types.append(None)
					landing_loc_nths.append(None)

			self.launcher_maiden_flight = ';;'.join(maiden_flights)
			self.launcher_last_flight = ';;'.join(last_flights)
			self.launcher_landing_attempt = ';;'.join(landing_attempts)
			self.launcher_landing_location = ';;'.join(landing_locs)
			self.landing_type = ';;'.join(landing_types)
			self.launcher_landing_location_nth_landing = ';;'.join(
				landing_loc_nths)
		elif stage_count == 1:
			launcher_json = launch_json['rocket']['launcher_stage'][0]

			self.launcher_stage_id = launcher_json['id']
			self.launcher_stage_type = launcher_json['type']
			self.launcher_stage_is_reused = launcher_json['reused']
			self.launcher_stage_flight_number = launcher_json[
				'launcher_flight_number']
			self.launcher_stage_turn_around = launcher_json[
				'turn_around_time_days']

			self.launcher_is_flight_proven = launcher_json['launcher'][
				'flight_proven']
			self.launcher_serial_number = launcher_json['launcher'][
				'serial_number']

			try:
				self.launcher_maiden_flight = timestamp_to_unix(
					launcher_json['launcher']['first_launch_date'])
				self.launcher_last_flight = timestamp_to_unix(
					launcher_json['launcher']['last_launch_date'])
			except:
				self.launcher_maiden_flight = None
				self.launcher_last_flight = None

			if launcher_json['landing'] is not None:
				landing_json = launcher_json['landing']

				try:
					self.launcher_landing_attempt = landing_json['attempt']
					self.launcher_landing_location = landing_json['location'][
						'abbrev']
					self.landing_type = landing_json['type']['abbrev']
					self.launcher_landing_location_nth_landing = landing_json[
						'location']['successful_landings']
				except:
					self.launcher_landing_attempt = None
					self.launcher_landing_location = None
					self.landing_type = None
					self.launcher_landing_location_nth_landing = None
			else:
				self.launcher_landing_attempt = None
				self.launcher_landing_location = None
				self.landing_type = None
				self.launcher_landing_location_nth_landing = None
		else:
			self.launcher_stage_id = None
			self.launcher_stage_type = None
			self.launcher_stage_is_reused = None
			self.launcher_stage_flight_number = None
			self.launcher_stage_turn_around = None
			self.launcher_is_flight_proven = None
			self.launcher_serial_number = None
			self.launcher_maiden_flight = None
			self.launcher_last_flight = None
			self.launcher_landing_attempt = None
			self.launcher_landing_location = None
			self.landing_type = None
			self.launcher_landing_location_nth_landing = None

		if launch_json['rocket']['spacecraft_stage'] not in (None, []):
			spacecraft = launch_json['rocket']['spacecraft_stage']
			self.spacecraft_id = spacecraft['id']
			self.spacecraft_sn = spacecraft['spacecraft']['serial_number']
			self.spacecraft_name = spacecraft['spacecraft'][
				'spacecraft_config']['name']

			if spacecraft['launch_crew'] not in (None, []):
				astronauts = set()
				for crew_member in spacecraft['launch_crew']:
					astronauts.add(
						f"{crew_member['astronaut']['name']}:{crew_member['role']}"
					)

				self.spacecraft_crew = ','.join(astronauts)
				self.spacecraft_crew_count = len(astronauts)

			try:
				self.spacecraft_maiden_flight = timestamp_to_unix(
					spacecraft['spacecraft']['spacecraft_config']
					['maiden_flight'])
			except:
				self.spacecraft_maiden_flight = None
		else:
			self.spacecraft_id = None
			self.spacecraft_sn = None
			self.spacecraft_name = None
			self.spacecraft_crew = None
			self.spacecraft_crew_count = None
			self.spacecraft_maiden_flight = None

		if launch_json['mission'] is not None:
			self.mission_name = launch_json['mission']['name']
			self.mission_type = launch_json['mission']['type']

			self.mission_description = launch_json['mission']['description']

			if self.mission_description not in (None, ''):
				try:
					sentences = tokenize.sent_tokenize(
						self.mission_description)
				except LookupError:
					logging.warning(
						'нет nltk. загружем'
					)

					import nltk
					nltk.download("punkt")

					try:
						sentences = tokenize.sent_tokenize(
							self.mission_description)
					except:
						logging.exception(
							"nltk умер"
						)
						return
			else:
				sentences = []

				parsed_description = ''
				max_idx = len(sentences) - 1

				for enum, sentence in enumerate(sentences):
					if len(parsed_description) + len(sentence) > 350:
						if enum == 0:
							parsed_description = sentence

						break

					parsed_description += sentence

					if enum != max_idx:
						if not len(parsed_description) + len(
							sentences[enum + 1]) > 350:
							parsed_description += ' '

				self.mission_description = parsed_description

			if launch_json['mission']['orbit'] is not None:
				self.mission_orbit = launch_json['mission']['orbit']['name']
				self.mission_orbit_abbrev = launch_json['mission']['orbit'][
					'abbrev']
			else:
				self.mission_orbit = None
				self.mission_orbit_abbrev = None

		else:
			self.mission_name = None
			self.mission_type = None
			self.mission_description = None
			self.mission_orbit = None
			self.mission_orbit_abbrev = None

		self.pad_name = launch_json['pad']['name']
		if 'Rocket Lab' in self.pad_name:
			self.pad_name = self.pad_name.replace('Rocket Lab', 'RL')

		self.location_name = launch_json['pad']['location']['name']
		self.location_country_code = launch_json['pad']['location'][
			'country_code']

		try:
			self.pad_nth_launch = launch_json['pad']['total_launch_count']
			self.location_nth_launch = launch_json['pad']['location'][
				'total_launch_count']
			self.agency_nth_launch = launch_json['agency_launch_attempt_count']
			self.agency_nth_launch_year = launch_json[
				'agency_launch_attempt_count_year']
		except KeyError:
			self.pad_nth_launch = None
			self.location_nth_launch = None
			self.agency_nth_launch = None
			self.agency_nth_launch_year = None

		if 'orbital_launch_attempt_count_year' in launch_json:
			self.orbital_nth_launch_year = launch_json[
				'orbital_launch_attempt_count_year']
		else:
			self.orbital_nth_launch_year = None


def construct_params(PARAMS: dict) -> str:
	param_url = ''
	if PARAMS is not None:
		for enum, keyvals in enumerate(PARAMS.items()):
			key, val = keyvals[0], keyvals[1]
			param_url += f'?{key}={val}' if enum == 0 else f'&{key}={val}'

	return param_url


def clean_launch_db(last_update, db_path):
	conn = sqlite3.connect(os.path.join(db_path, 'launchbot-data.db'))
	conn.row_factory = sqlite3.Row
	cursor = conn.cursor()

	cursor.execute(
		'SELECT name FROM sqlite_master WHERE type = ? AND name = ?',
		('table', 'launches'))
	if len(cursor.fetchall()) == 0:
		return

	cursor.execute(
		'SELECT unique_id FROM launches WHERE launched = 0 AND last_updated < ? AND net_unix > ?',
		(last_update, int(time.time())))

	deleted_launches = set()
	for launch_row in cursor.fetchall():
		deleted_launches.add(launch_row[0])

	if len(deleted_launches) == 0:
		logging.debug('бд чистая')
		return

	logging.info(
		f'удалено {len(deleted_launches)}'
	)
	try:
		cursor.execute(
			'DELETE FROM launches WHERE launched = 0 AND last_updated < ? AND net_unix > ?',
			(last_update, int(time.time())))

		logging.info(f'удалено {deleted_launches}')
	except Exception:
		logging.exception('ошибка при удалении')

	conn.commit()
	conn.close()


def ll2_api_call(data_dir: str, scheduler: BackgroundScheduler,
	bot_username: str, bot: 'telegram.bot.Bot'):
	DEBUG_API = False

	API_URL = 'https://ll.thespacedevs.com'
	API_VERSION = '2.1.0'
	API_REQUEST = 'launch/upcoming'
	PARAMS = {'mode': 'detailed', 'limit': 30}

	API_CALL = f'{API_URL}/{API_VERSION}/{API_REQUEST}/{construct_params(PARAMS)}'

	headers = {'user-agent': f'telegram-{bot_username}'}

	if DEBUG_API and os.path.isfile(os.path.join(data_dir, 'debug-json.json')):
		with open(os.path.join(data_dir, 'debug-json.json'), 'r') as json_file:
			api_json = json.load(json_file)

		rec_data = 0
		time.sleep(1.5)
	else:
		try:
			session = requests.Session()
			session.headers = headers
			session.mount("https://",
				HTTPAdapter(pool_connections=1, pool_maxsize=2))

			t0 = time.time()
			API_RESPONSE = session.get(API_CALL, timeout=5)
			rec_data = len(API_RESPONSE.content)

			tdelta = time.time() - t0
		except Exception as error:
			logging.warning(f'ошибка {error}')
			return ll2_api_call(data_dir=data_dir,
				scheduler=scheduler,
				bot_username=bot_username,
				bot=bot)

		try:
			api_json = json.loads(API_RESPONSE.text)
			if DEBUG_API:
				with open(os.path.join(data_dir, 'debug-json.json'),
					'w') as jsonf:
					json.dump(api_json, jsonf, indent=4)
		except Exception as json_parse_error:
			logging.exception(f'ошибка json{json_parse_error}')
			with open(
				os.path.join(data_dir, f'error-json-{int(time.time())}.txt'),
				'w') as ejson:
				ejson.write(API_RESPONSE.text)
			time.sleep(60)

			return ll2_api_call(data_dir=data_dir,
				scheduler=scheduler,
				bot_username=bot_username,
				bot=bot)

	api_updated = int(time.time())

	launch_obj_set = set()

	t0 = time.time()

	for launch in api_json['results']:
		try:
			launch_obj_set.add(LaunchLibrary2Launch(launch))
		except:
			pass

	tdelta = time.time() - t0

	postponed_launches = update_launch_db(launch_set=launch_obj_set,
		db_path=data_dir,
		bot_username=bot_username,
		api_update=api_updated)

	clean_launch_db(last_update=api_updated, db_path=data_dir)

	if len(postponed_launches) > 0:
		logging.info(f'Found {len(postponed_launches)} postponed launches!')
		for postpone_tuple in postponed_launches:
			launch_object = postpone_tuple[0]

			notify_list, sent_notification_ids = postpone_notification(
				db_path=data_dir, postpone_tuple=postpone_tuple, bot=bot)
			
			remove_previous_notification(db_path=data_dir,
				launch_id=launch_object.unique_id,
				notify_set=notify_list,
				bot=bot)

			msg_id_str = ','.join(sent_notification_ids)
			store_notification_identifiers(db_path=data_dir,
				launch_id=launch_object.unique_id,
				identifiers=msg_id_str)

			update_stats_db(stats_update={'notifications': len(notify_list)},
				db_path=data_dir)

	update_stats_db(stats_update={
		'api_requests': 1,
		'db_updates': 1,
		'data': rec_data,
		'last_api_update': api_updated
	},
		db_path=data_dir)

	next_api_update = api_call_scheduler(db_path=data_dir,
		scheduler=scheduler,
		ignore_60=True,
		bot_username=bot_username,
		bot=bot)

	notification_send_scheduler(db_path=data_dir,
		next_api_update_time=next_api_update,
		scheduler=scheduler,
		bot_username=bot_username,
		bot=bot)


def api_call_scheduler(db_path: str, scheduler: BackgroundScheduler,
	ignore_60: bool, bot_username: str, bot: 'telegram.bot.Bot'):

	def schedule_call(unix_timestamp: int) -> int:
		if unix_timestamp <= int(time.time()):
			unix_timestamp = int(time.time()) + 3

		until_update = unix_timestamp - int(time.time())

		next_update_dt = datetime.datetime.fromtimestamp(unix_timestamp)

		scheduler.add_job(ll2_api_call,
			'date',
			run_date=next_update_dt,
			args=[db_path, scheduler, bot_username, bot],
			id=f'api-{unix_timestamp}')

		return unix_timestamp

	def require_immediate_update(cursor: sqlite3.Cursor) -> tuple:
		try:
			cursor.execute('SELECT last_api_update FROM stats')
		except sqlite3.OperationalError:
			return (True, None)

		last_update = cursor.fetchall()[0][0]
		if last_update in ('', None):
			return (True, None)

		return (True,
			None) if time.time() > last_update + UPDATE_PERIOD * 60 * 2 else (
			False, last_update)

	UPDATE_PERIOD = 15

	conn = sqlite3.connect(os.path.join(db_path, 'launchbot-data.db'))
	cursor = conn.cursor()

	db_status = require_immediate_update(cursor)
	update_immediately, last_update = db_status[0], db_status[1]

	if update_immediately:
		return schedule_call(int(time.time()) + 5)

	update_delta = int(time.time()) - last_update
	last_updated_str = time_delta_to_legible_eta(update_delta,
		full_accuracy=False)

	select_fields = 'net_unix, launched, status_state'
	select_fields += ', notify_24h, notify_12h, notify_60min, notify_5min'
	notify_window = int(time.time()) - 60 * 5

	try:
		cursor.execute(
			f'SELECT {select_fields} FROM launches WHERE net_unix >= ?',
			(notify_window, ))
		query_return = cursor.fetchall()
	except sqlite3.OperationalError:
		query_return = set()

	conn.close()

	if len(query_return) == 0:
		os.rename(
			os.path.join(db_path, 'launchbot-data.db'),
			os.path.join(db_path,
			f'launchbot-data-sched-error-{int(time.time())}.db'))

		return schedule_call(int(time.time()) + 5)

	query_return.sort(key=lambda tup: tup[0])
	notif_times, time_map = set(), {
		0: 24 * 3600,
		1: 12 * 3600,
		2: 3600,
		3: 5 * 60
	}
	notif_time_map = dict()

	for launch_row in query_return:
		launch_status = launch_row[2]
		if launch_status == 'TBD':
			continue
		if not launch_row[1] and time.time() - launch_row[0] < 60:
			notif_times.add(launch_row[0] + 5 * 60)

			check_time = launch_row[0] + 5 * 60
			if check_time not in notif_time_map.keys():
				notif_time_map[check_time] = {-1}
			else:
				notif_time_map[check_time].add(-1)

		for enum, notif_bool in enumerate(launch_row[3::]):
			if not notif_bool:
				check_time = launch_row[0] - time_map[enum] - 60
				if check_time - int(time.time()) < 60 and ignore_60:
					pass
				elif check_time < time.time():
					pass
				else:
					notif_times.add(check_time)

					if check_time not in notif_time_map.keys():
						notif_time_map[check_time] = {enum}
					else:
						notif_time_map[check_time].add(enum)

	next_notif = min(notif_times)

	next_notif_earliest_type = max(notif_time_map[next_notif])
	next_notif_type = {
		0: '24h',
		1: '12h',
		2: '60m',
		3: '5m',
		-1: 'LCHECK'
	}[next_notif_earliest_type]

	until_next_notif = next_notif - int(time.time())
	next_notif_send_time = time_delta_to_legible_eta(
		time_delta=until_next_notif, full_accuracy=False)

	next_notif = datetime.datetime.fromtimestamp(next_notif)

	if next_notif_type == '24h':
		if until_next_notif >= 6 * 3600:
			upd_period_mult = 16
		else:
			upd_period_mult = 12
	elif next_notif_type == '12h':
		upd_period_mult = 12
	elif next_notif_type == '60m':
		if until_next_notif >= 4 * 3600:
			upd_period_mult = 8
		else:
			upd_period_mult = 4
	elif next_notif_type == '5m':
		upd_period_mult = 1.35
	elif next_notif_type == 'LCHECK':
		upd_period_mult = 1.35
	else:
		upd_period_mult = 4

	to_next_update = int(UPDATE_PERIOD * upd_period_mult) * 60 - update_delta
	next_auto_update = int(time.time()) + to_next_update
	notif_times.add(next_auto_update)

	next_api_update = min(notif_times)

	rd = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
	rd.flushdb()
	rd.set('next-api-update', next_api_update)

	return schedule_call(next_api_update)


if __name__ == '__main__':
	BOT_USERNAME = 'SpaceResearch'
	DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')

	if not os.path.isdir(DATA_DIR):
		os.makedirs(DATA_DIR)

	scheduler = BackgroundScheduler()
	scheduler.start()

	api_call_scheduler(db_path=DATA_DIR,
		ignore_60=False,
		scheduler=scheduler,
		bot_username=BOT_USERNAME,
		bot=None)

	while True:
		time.sleep(10)
