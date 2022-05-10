import os
import time
import ujson as json


def first_run(data_dir: str):
	if not os.path.isdir(data_dir):
		os.makedirs(data_dir)


def store_config(config_json: dict, data_dir: str):
	with open(os.path.join(data_dir, 'bot-config.json'), 'w') as config_file:
		json.dump(config_json, config_file, indent=4)


def create_config(data_dir: str):
	if not os.path.isdir(data_dir):
		first_run(data_dir)

	with open(os.path.join(data_dir, 'bot-config.json'), 'w') as config_file:

		bot_token = input('Enter bot token: ')
		print()

		config = {
			'bot_token': bot_token,
			'owner': 0,
			'redis': {
			'host': 'localhost',
			'port': 6379,
			'db_num': 0
			},
			'local_api_server': {
			'enabled': False,
			'logged_out': False,
			'address': None
			}
		}

		json.dump(config, config_file, indent=4)


def load_config(data_dir: str) -> dict:
	if not os.path.isfile(os.path.join(data_dir, 'bot-config.json')):
		create_config(data_dir)

	with open(os.path.join(data_dir, 'bot-config.json'), 'r') as config_file:
		try:
			return json.load(config_file)
		except:
			print(
				'создаем json'
			)

			create_config(data_dir)
			return load_config(data_dir)

	with open(os.path.join(data_dir, 'bot-config.json'), 'r') as config_file:
		return json.load(config_file)


def repair_config(data_dir: str) -> dict:
	config_keys = {'bot_token', 'owner', 'redis', 'local_api_server'}

	full_config = {
		'bot_token': 0,
		'owner': 0,
		'redis': {
		'host': 'localhost',
		'port': 6379,
		'db_num': 0
		},
		'local_api_server': {
		'enabled': False,
		'logged_out': False,
		'address': None
		}
	}

	config = load_config(data_dir=data_dir)

	set_diff = config_keys.difference(set(config.keys()))
	if set_diff == set():
		return config

	for key, val in full_config.items():
		if key not in config:
			config[key] = val

	return config
