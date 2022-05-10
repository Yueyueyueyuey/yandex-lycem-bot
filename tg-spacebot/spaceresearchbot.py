import os
import sys
import time
import datetime
import logging
import math
import inspect
import random
import sqlite3
import signal
import argparse

from timeit import default_timer as timer
from http.client import HTTPConnection

import git
import psutil
import cursor
import pytz
import coloredlogs
import telegram
import redis
from uptime import uptime
from timezonefinder import TimezoneFinder
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR
from telegram import ReplyKeyboardRemove, ForceReply
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
from telegram.ext import CallbackQueryHandler

from api import api_call_scheduler
from config import load_config, store_config, repair_config
from db import (update_stats_db, create_chats_db)
from tools import (anonymize_id, time_delta_to_legible_eta,
	map_country_code_to_flag, timestamp_to_legible_date_string,
	short_monospaced_text, reconstruct_message_for_markdown,
	reconstruct_link_for_markdown, suffixed_readable_int, retry_after)
from timezone import (load_locale_string, remove_time_zone_information,
	update_time_zone_string, update_time_zone_value, load_time_zone_status)
from notifications import (get_user_notifications_status, toggle_notification,
	update_notif_preference, get_notif_preference, toggle_launch_mute,
	clean_chats_db)
global VERSION, OWNER
global BOT_ID, BOT_USERNAME
global DATA_DIR, STARTUP_TIME


rd = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

try:
	ret = rd.setex(name='foo', value='bar', time=datetime.timedelta(seconds=1))
except redis.exceptions.ConnectionError:
	sys.exit()


def api_update_on_restart():
	conn = sqlite3.connect(os.path.join(DATA_DIR, 'launchbot-data.db'))
	cursor_ = conn.cursor()

	try:
		cursor_.execute('UPDATE stats SET last_api_update = ?', (None, ))
	except sqlite3.OperationalError:
		pass
	conn.commit()
	conn.close()


def admin_handler(update, context):

	def invalid_command():
		args_list = ("`export-logs`", "`export-db`", "`force-api-update`",
			"`git-pull`", "`restart`", "`feedbackreply`")

		context.bot.send_message(chat_id=chat.id,
			parse_mode="Markdown",
			text=f"Неправильная команда. Список команд админа: {', '.join(args_list)}.")

	def restart_program():
		try:
			proc = psutil.Process(os.getpid())
			for handler in proc.open_files() + proc.connections():
				os.close(handler.fd)
		except Exception as error:
			pass
		python = sys.executable
		os.execl(python, python, *sys.argv)

	try:
		chat = update.message.chat
	except AttributeError:
		return
	if update.message.text == '/debug export-logs':
		log_path = os.path.join(DATA_DIR, 'log-file.log')
		log_fsz = os.path.getsize(log_path) / 10**6

		context.bot.send_message(chat_id=chat.id,
			text=f'Экспорт логов {log_fsz:.2f}',
			parse_mode='Markdown')

		with open(log_path, 'rb') as log_file:
			context.bot.send_document(chat_id=chat.id,
				document=log_file,
				filename=f'log-export-{int(time.time())}.log')

	elif update.message.text == '/debug export-db':
		context.bot.send_message(chat_id=chat.id,
			text='Экспорт дб')

		with open(os.path.join(DATA_DIR, 'launchbot-data.db'),
			'rb') as db_file:
			context.bot.send_document(chat_id=chat.id,
				document=db_file,
				filename=f'db-export-{int(time.time())}.db')

	elif update.message.text == '/debug restart':
		running = time_delta_to_legible_eta(int(time.time() - STARTUP_TIME),
			True)

		context.bot.send_message(chat_id=chat.id,
			text=f'Рестарт. Время работы: {running}.',
			parse_mode='Markdown')
		restart_program()

	elif update.message.text == '/debug git-pull':
		context.bot.send_message(chat_id=chat.id,
			text='pull в ветку',
			parse_mode='Markdown')

		repo = git.Repo('../')
		current = repo.head.commit
		repo.remotes.origin.pull()

		last_commit = repo.heads[0].commit.hexsha
		context.bot.send_message(chat_id=chat.id,
				text=f' pull завершен',
				parse_mode='Markdown')

		repo.close()

	elif update.message.text == '/debug force-api-update':
		api_update_on_restart()
		context.bot.send_message(chat_id=chat.id,
			text='DB обновлено')

	elif "/debug feedbackreply" in update.message.text:
		command = update.message.text.split(" ")
		if len(command) < 4:
			invalid_command()
			return
		chatId = command[2]
		textContent = command[3::]
		if chatId != "ADMIN":
			try:
				chatId = int(chatId)
			except:
				context.bot.send_message(chat_id=chat.id,
					parse_mode="Markdown",
					text=
					f"{chatId}` не int"
											)

				return
		else:
			chatId = chat.id

		feedbackReplyMessage = f"Фидбэк бота \n\n{' '.join(textContent)}"
		feedbackReplyMessage += "Для ответа на сообщения использовать /feedback"

		try:
			context.bot.send_message(chat_id=chatId,
				parse_mode='Markdown',
				text=feedbackReplyMessage)
		except Exception as e:
			context.bot.send_message(chat_id=chat.id,
				parse_mode='Markdown',
				text=f"Ошибка отправки сообщения: {e}")
		context.bot.send_message(chat_id=chat.id,
			parse_mode='Markdown',
			text=f"Отправить сообщение в чат `{chatId}`!")

	else:
		invalid_command()


def generic_update_handler(update, context):
	if update.message is None:
		if update.channel_post is not None:
			return
		return

	chat = update.message.chat
	if update.message.left_chat_member not in (None, False):
		if update.message.left_chat_member.id == BOT_ID:
			conn = sqlite3.connect(os.path.join(DATA_DIR, 'launchbot-data.db'))
			cursor_ = conn.cursor()

			try:
				cursor_.execute("DELETE FROM chats WHERE chat = ?",
					(chat.id, ))
			except Exception as error:
				logging.exception(
					f'Ошибка удаления чата из бд {error}')

			conn.commit()
			conn.close()

	elif update.message.group_chat_created not in (None, False):
		start(update, context)

	elif update.message.migrate_from_chat_id not in (None, False):
		conn = sqlite3.connect(os.path.join(DATA_DIR, 'launchbot-data.db'))
		cursor_ = conn.cursor()

		try:
			cursor_.execute('UPDATE chats SET chat = ? WHERE chat = ?',
				(chat.id, update.message.migrate_from_chat_id))
		except Exception:
			pass

		conn.commit()
		conn.close()

	elif update.message.new_chat_members not in (None, False):
		if BOT_ID in [user.id for user in update.message.new_chat_members]:
			start(update, context)


def command_pre_handler(update, context, skip_timer_handle):
	try:
		chat = update.message.chat
	except AttributeError:
		pass
		return False

	if update.message.from_user.id in ignored_users:
		return False

	if update.message.author_signature in ignored_users:
		return False
	try:
		try:
			chat_type = chat.type
		except KeyError:
			chat_type = context.bot.getChat(chat).type

	except telegram.error.RetryAfter as error:
		retry_after(error.retry_after)

		return False

	except telegram.error.TimedOut as error:
		return False

	except telegram.error.ChatMigrated as error:
		conn = sqlite3.connect(os.path.join(DATA_DIR, 'launchbot-data.db'))
		cursor_ = conn.cursor()

		cursor_.execute("UPDATE chats SET chat = ? WHERE chat = ?",
			(error.new_chat_id, chat.id))
		conn.commit()
		conn.close()

		return True

	except telegram.error.Unauthorized as error:
		clean_chats_db(DATA_DIR, chat.id)
		return False

		conn = sqlite3.connect(os.path.join(DATA_DIR, 'launchbot-data.db'))
		cursor_ = conn.cursor()

		cursor_.execute(
			"DELETE FROM chats WHERE chat = ?",
			(chat.id),
		)
		conn.commit()
		conn.close()

		return False

	try:
		cmd = update.message.text.split(' ')[0]
	except AttributeError:
		return True
	except KeyError:
		return False

	if not skip_timer_handle:
		if not timer_handle(update, context, cmd, chat.id,
			update.message.from_user.id):
			blocked_user = update.message.from_user.id
			blocked_chat = chat.id
			return False

	if chat_type != 'private':
		try:
			all_admins = update.message.chat.all_members_are_administrators
		except Exception:
			all_admins = False

		if not all_admins:
			sender = context.bot.get_chat_member(chat.id,
				update.message.from_user.id)
			if sender.status not in ('creator', 'administrator'):
				bot_chat_specs = context.bot.get_chat_member(
					chat.id,
					context.bot.getMe().id)

				return False

	return True


def callback_handler(update, context):
	def update_main_view(chat, msg, text_refresh):
		providers = set()
		for val in provider_by_cc.values():
			for provider in val:
				if provider in provider_name_map.keys():
					providers.add(provider_name_map[provider])
				else:
					providers.add(provider)

		notification_statuses = get_user_notifications_status(
			DATA_DIR, chat, providers, provider_name_map)

		disabled_count, all_flag = 0, False
		if 0 in notification_statuses.values():
			disabled_count = 1

		try:
			if notification_statuses['All'] == 1:
				all_flag = True
		except KeyError:
			pass

		rand_planet = '🌍'

		if all_flag:
			toggle_text = 'включить' if disabled_count != 0 else 'выключить'
		elif not all_flag:
			toggle_text = 'включить'

		global_text = f'{rand_planet} Нажмите {toggle_text} всё'

		keyboard = InlineKeyboardMarkup(inline_keyboard=[[
			InlineKeyboardButton(text=global_text,
			callback_data=f'notify/toggle/all/all')
		],
			[
			InlineKeyboardButton(text='🇪🇺 Европа',
			callback_data=f'notify/list/EU'),
			InlineKeyboardButton(text='🇺🇸 США',
			callback_data=f'notify/list/USA')
			],
			[
			InlineKeyboardButton(text='🇷🇺 Россия',
			callback_data=f'notify/list/RUS'),
			InlineKeyboardButton(text='🇨🇳 Китай',
			callback_data=f'notify/list/CHN')
			],
			[
			InlineKeyboardButton(text='🇮🇳 Индия',
			callback_data=f'notify/list/IND'),
			InlineKeyboardButton(text='🇯🇵 Япония',
			callback_data=f'notify/list/JPN')
			],
			[
			InlineKeyboardButton(text='🇹🇼 Тайвань',
			callback_data=f'notify/list/TWN')
			],
			[
			InlineKeyboardButton(text='Настройки',
			callback_data=f'prefs/main_menu')
			],
			[
			InlineKeyboardButton(text='Сохранить и выйти',
			callback_data=f'notify/done')
			]])

		if text_refresh:
			message_text = '''
			Настройки уведомлений

			Вы можете редактировать свой временной пояс, от кого хотите получать сообщения
			и настройки уведомлений. 

			🔔 = *Включено* 
			🔕 = *Выключено* 
			'''

			try:
				query.edit_message_text(text=inspect.cleandoc(message_text),
					reply_markup=keyboard,
					parse_mode='Markdown')
			except:
				pass
		else:
			try:
				query.edit_message_reply_markup(reply_markup=keyboard)
			except:
				pass

	def update_list_view(msg, chat, provider_list):
		providers = set()
		for provider in provider_by_cc[country_code]:
			if provider in provider_name_map.keys():
				providers.add(provider_name_map[provider])
			else:
				providers.add(provider)

		notification_statuses = get_user_notifications_status(
			DATA_DIR, chat, providers, provider_by_cc)
		disabled_count = 0

		for key, val in notification_statuses.items():
			if val == 0 and key != 'All':
				disabled_count += 1
				break

		local_text = 'Нажмите чтобы принять всё' if disabled_count != 0 else 'Нажмите чтобы отменить всё'

		inline_keyboard = [[
			InlineKeyboardButton(
			text=f'{map_country_code_to_flag(country_code)} {local_text}',
			callback_data=
			f'notify/toggle/country_code/{country_code}/{country_code}')
		]]
		provider_list = list(provider_list)
		provider_list.sort(key=len)
		current_row = 0 
		for provider, i in zip(provider_list, range(len(provider_list))):
			if provider in provider_name_map.keys():
				provider_db_name = provider_name_map[provider]
			else:
				provider_db_name = provider

			notification_icon = {
				0: '🔕',
				1: '🔔'
			}[notification_statuses[provider_db_name]]

			if i % 2 == 0 or i == 0:
				current_row += 1
				inline_keyboard.append([
					InlineKeyboardButton(
					text=f'{provider} {notification_icon}',
					callback_data=f'notify/toggle/lsp/{provider}/{country_code}'
					)
				])
			else:
				if len(provider) <= len('Virgin Orbit'):
					inline_keyboard[current_row].append(
						InlineKeyboardButton(
						text=f'{provider} {notification_icon}',
						callback_data=
						f'notify/toggle/lsp/{provider}/{country_code}'))
				else:
					current_row += 1
					inline_keyboard.append([
						InlineKeyboardButton(
						text=f'{provider} {notification_icon}',
						callback_data=
						f'notify/toggle/lsp/{provider}/{country_code}')
					])

		inline_keyboard.append([
			InlineKeyboardButton(text='Вернутся в меню',
			callback_data='notify/main_menu')
		])
		keyboard = InlineKeyboardMarkup(inline_keyboard=inline_keyboard)

		try:
			query.edit_message_reply_markup(reply_markup=keyboard)
		except telegram.error.BadRequest:
			pass


	try:
		query = update.callback_query
		query_data = update.callback_query.data
		from_id = update.callback_query.from_user.id
	except Exception as caught_exception:
		pass
		return

	start = timer()
	input_data = query_data.split('/')
	msg = update.callback_query.message
	chat = msg.chat.id
	try:
		chat_type = msg.chat.type
	except:
		chat_type = context.bot.getChat(chat).type

	if chat_type != 'private':
		try:
			all_admins = msg.chat.all_members_are_administrators
		except:
			all_admins = False

		if not all_admins:
			try:
				sender = context.bot.get_chat_member(chat, from_id)
			except telegram.error.Unauthorized as error:
				if 'bot was kicked' in error.message:
					clean_chats_db(DATA_DIR, chat)
					return
				else:
					return

			if sender.status == 'left':
				try:
					query.answer(
						text="Отправьте команду ещё раз")
				except Exception as error:
					pass
					return
				return

			if sender.status not in ('creator', 'administrator'):
				try:
					query.answer(
						text="Кнопки только для админов")
				except Exception as error:
					pass
				return

	if input_data[0] not in ('notify', 'mute', 'next_flight', 'schedule',
		'prefs', 'stats'):
		return

	if input_data[0] == 'notify':
		if input_data[1] == 'list':
			country_code = input_data[2]
			try:
				provider_list = provider_by_cc[country_code]
			except:
				pass
				return

			update_list_view(msg, chat, provider_list)

			try:
				query.answer(text=f'{map_country_code_to_flag(country_code)}')
			except Exception as error:
				pass

		elif input_data[1] == 'main_menu':
			try:
				if input_data[2] == 'refresh_text':
					update_main_view(chat, msg, True)
			except:
				update_main_view(chat, msg, False)

			try:
				query.answer(text='Вернуться в меню')
			except Exception as error:
				pass
		elif input_data[1] == 'toggle':

			def get_new_notify_group_toggle_state(
					toggle_type, country_code, chat):
				providers = set()
				if toggle_type == 'all':
					for val in provider_by_cc.values():
						for provider in val:
							providers.add(provider)

				elif toggle_type == 'country_code':
					for provider in provider_by_cc[country_code]:
						providers.add(provider)

				notification_statuses = get_user_notifications_status(
					DATA_DIR, chat, providers, provider_name_map)
				disabled_count = 0

				for key, val in notification_statuses.items():
					if toggle_type == 'country_code' and key != 'All':
						if val == 0:
							disabled_count += 1
							break

					elif toggle_type in ('all', 'lsp'):
						if val == 0:
							disabled_count += 1
							break

				return 1 if disabled_count != 0 else 0

			if input_data[2] not in ('country_code', 'lsp', 'all'):
				return
			if input_data[2] == 'all':
				all_toggle_new_status = get_new_notify_group_toggle_state(
					'all', None, chat)
			else:
				country_code = input_data[3]
				if input_data[2] == 'country_code':
					all_toggle_new_status = get_new_notify_group_toggle_state(
						'country_code', country_code, chat)
				else:
					all_toggle_new_status = 0
			new_status = toggle_notification(DATA_DIR, chat, input_data[2],
				input_data[3], all_toggle_new_status, provider_by_cc,
				provider_name_map)

			if input_data[2] == 'lsp':
				reply_text = {
					1: f'Показать сообщение для {input_data[3]}',
					0: f'Скрыть сообщение для {input_data[3]}'
				}[new_status]
			elif input_data[2] == 'country_code':
				reply_text = {
					1:
					f'Показать сообщение для {map_country_code_to_flag(input_data[3])}',
					0:
					f'Скрыть сообщение для {map_country_code_to_flag(input_data[3])}'
				}[new_status]
			elif input_data[2] == 'all':
				reply_text = {
					1: 'Включить все сообщения',
					0: 'Отключиться все сообщения'
				}[new_status]

			try:
				query.answer(text=reply_text, show_alert=True)
			except Exception as error:
				pass
			if input_data[2] != 'all':
				country_code = input_data[4]
				try:
					provider_list = provider_by_cc[country_code]
				except:
					return

				update_list_view(msg, chat, provider_list)
			else:
				update_main_view(chat, msg, False)

			if rd.exists(f'next-{chat}-maxindex'):
				max_index = rd.get(f'next-{chat}-maxindex')

				rd.expire(f'next-{chat}-maxindex',
					datetime.timedelta(seconds=0.1))

				for i in range(0, int(max_index)):
					rd.expire(f'next-{chat}-{i}',
						datetime.timedelta(seconds=0.1))
					rd.expire(f'next-{chat}-{i}-net',
						datetime.timedelta(seconds=0.1))

		elif input_data[1] == 'done':
			reply_text = 'Все готово'

			msg_text = f'''
			Настройки уведомлений
			Если вы захотите изменить настройки, то отправьте команду /notify@{BOT_USERNAME}
			'''

			inline_keyboard = [[
				InlineKeyboardButton(text="Вернуться к настройкам",
				callback_data='notify/main_menu')
			]]
			keyboard = InlineKeyboardMarkup(inline_keyboard=inline_keyboard)
			try:
				query.edit_message_text(text=inspect.cleandoc(msg_text),
					reply_markup=keyboard,
					parse_mode='Markdown')
			except telegram.error.BadRequest:
				pass

			try:
				query.answer(text=reply_text)
			except Exception as error:
				pass

	elif input_data[0] == 'mute':
		new_toggle_state = 0 if int(input_data[2]) == 1 else 1
		new_text = {
			0: 'Размутить',
			1: 'Замутить'
		}[new_toggle_state]
		new_data = f'mute/{input_data[1]}/{new_toggle_state}'
		inline_keyboard = [[
			InlineKeyboardButton(text=new_text, callback_data=new_data)
		]]
		keyboard = InlineKeyboardMarkup(inline_keyboard=inline_keyboard)

		callback_text = 'уведомления замутены' if input_data[
			2] == '1' else 'уведомления размутены'

		try:
			query.edit_message_reply_markup(reply_markup=keyboard)

			try:
				query.answer(text=callback_text)
			except Exception as error:
				pass
		except Exception as exception:

			try:
				context.bot.send_message(chat,
					callback_text,
					parse_mode='Markdown')
			except Exception as exception:
				pass

		toggle_launch_mute(db_path=DATA_DIR,
			chat=chat,
			launch_id=input_data[1],
			toggle=int(input_data[2]))

	elif input_data[0] == 'next_flight':
		if input_data[1] not in ('next', 'prev', 'refresh'):
			return

		current_index = input_data[2]
		if input_data[1] == 'next':
			new_message_text, keyboard = generate_next_flight_message(
				chat,
				int(current_index) + 1)

		elif input_data[1] == 'prev':
			new_message_text, keyboard = generate_next_flight_message(
				chat,
				int(current_index) - 1)

		elif input_data[1] == 'refresh':
			try:
				new_message_text, keyboard = generate_next_flight_message(
					chat, int(current_index))

			except TypeError:
				new_message_text = 'Полетов не найдено. Попробуйте посмотреть у других компаний'
				inline_keyboard = []
				inline_keyboard.append([
					InlineKeyboardButton(
					text='Обновитьс настройки уведомлений',
					callback_data=f'notify/main_menu/refresh_text')
				])
				inline_keyboard.append([
					InlineKeyboardButton(text='Искать во всех полетах',
					callback_data=f'next_flight/refresh/0')
				])
				keyboard = InlineKeyboardMarkup(
					inline_keyboard=inline_keyboard)

			except Exception as e:
				new_message_text, keyboard = generate_next_flight_message(
					chat, int(current_index))

		query_reply_text = {
			'next': 'Следующий полет',
			'prev': 'Предыдущий полет',
			'refresh': 'Обновить данные'
		}[input_data[1]]

		try:
			query.edit_message_text(text=new_message_text,
				reply_markup=keyboard,
				parse_mode='MarkdownV2')
		except telegram.error.TelegramError as exception:
			if 'Message is not modified' in exception.message:
				pass
			else:
				pass

		try:
			query.answer(text=query_reply_text)
		except Exception as error:
			pass


	elif input_data[0] == 'schedule':
		if input_data[1] not in ('refresh', 'vehicle', 'mission'):
			return

		if input_data[1] == 'refresh':
			try:
				new_schedule_msg, keyboard = generate_schedule_message(
					input_data[2], chat)
			except IndexError:  
				new_schedule_msg, keyboard = generate_schedule_message(
					'vehicle', chat)
		else:
			new_schedule_msg, keyboard = generate_schedule_message(
				input_data[1], chat)

		try:
			query.edit_message_text(text=new_schedule_msg,
				reply_markup=keyboard,
				parse_mode='MarkdownV2')

			if input_data[1] == 'refresh':
				query_reply_text = 'Расписание обновлено'
			else:
				query_reply_text = 'Расписание загружено' if input_data[
					1] == 'vehicle' else 'Миссии загружены'

			query.answer(text=query_reply_text)

		except telegram.error.TelegramError as exception:
			if 'Message is not modified' in exception.message:
				try:
					query_reply_text = 'Расписание обновлено. Изменений не найдено'
					query.answer(text=query_reply_text)
				except Exception as error:
					pass
				pass
			else:
				pass

	elif input_data[0] == 'prefs':
		if input_data[1] not in ('timezone', 'notifs', 'cmds', 'done',
			'main_menu'):
			return

		if input_data[1] == 'done':
			query.answer(text='Все готово')
			message_text = 'Ваши настройки сохранены'
			try:
				query.edit_message_text(text=message_text,
					reply_markup=None,
					parse_mode='Markdown')
			except telegram.error.BadRequest:
				pass

		elif input_data[1] == 'main_menu':
			query.answer(text='Вернуться к меню')
			message_text = f'''
			Основной чат

			Настройки уведомлений
			Настройки времени
			
			Если вы не выберете часовой пояс, то будет использоваться стандартный UTC+0
			'''

			keyboard = InlineKeyboardMarkup(inline_keyboard=[[
				InlineKeyboardButton(text=f'Настройки времени',
				callback_data='prefs/timezone/menu')
			],
				[
				InlineKeyboardButton(text='Настройки уведомлений',
				callback_data='prefs/notifs')
				],
				[
				InlineKeyboardButton(text='Вернуться в основное меню',
				callback_data='notify/main_menu/refresh_text')
				]])
			try:
				query.edit_message_text(text=inspect.cleandoc(message_text),
					reply_markup=keyboard,
					parse_mode='Markdown')
			except telegram.error.BadRequest:
				pass

		elif input_data[1] == 'notifs':
			if len(input_data) == 3:
				if input_data[2] in ('24h', '12h', '1h', '5m'):
					new_state = update_notif_preference(db_path=DATA_DIR,
						chat=chat,
						notification_type=input_data[2])

					query_reply_text = f'{input_data[2]} уведомлений '

					if 'h' in query_reply_text:
						query_reply_text = query_reply_text.replace(
							'h', ' часов')
					else:
						query_reply_text.replace('m', ' минут')

					query_reply_text += 'уведомления включены' if new_state == 1 else 'уведомления выключены'
					query.answer(text=query_reply_text, show_alert=True)

			notif_prefs = get_notif_preference(db_path=DATA_DIR, chat=chat)
			bell_dict = {1: '🔔', 0: '🔕'}

			new_prefs_text = '''
			Настройки уведомлений

			Стандартно уведомления отсылаются каждые 24, 12, 1 часа и за 5 минут до запуска
			Это можно изменить здесь

			🔔 = Выключено
			🔕 = Включено
			'''

			keyboard = InlineKeyboardMarkup(inline_keyboard=[[
				InlineKeyboardButton(
				text=f'24 часа до {bell_dict[notif_prefs[0]]}',
				callback_data='prefs/notifs/24h')
			],
				[
				InlineKeyboardButton(
				text=f'12 часов до {bell_dict[notif_prefs[1]]}',
				callback_data='prefs/notifs/12h')
				],
				[
				InlineKeyboardButton(
				text=f'1 час до {bell_dict[notif_prefs[2]]}',
				callback_data='prefs/notifs/1h')
				],
				[
				InlineKeyboardButton(
				text=f'5 минут до {bell_dict[notif_prefs[3]]}',
				callback_data='prefs/notifs/5m')
				],
				[
				InlineKeyboardButton(text='Вернуться в основное меню',
				callback_data='prefs/main_menu')
				]])
			try:
				query.edit_message_text(text=inspect.cleandoc(new_prefs_text),
					reply_markup=keyboard,
					parse_mode='Markdown')
			except telegram.error.BadRequest:
				pass

		elif input_data[1] == 'timezone':
			if input_data[2] == 'menu':
				text = f'''
				Настройки времени

				Здесь вы можете настроить часовой пояс для уведомлений.
				Вы можете использовать либо автоматический подбор либо ручной.

				Часовой пояс сейчас *UTC{load_time_zone_status(DATA_DIR, chat, readable=True)}*'''

				locale_string = load_locale_string(DATA_DIR, chat)
				if locale_string is not None:
					text += f' *({locale_string})*'

				keyboard = InlineKeyboardMarkup(inline_keyboard=[[
					InlineKeyboardButton(text='автоматический подбор',
					callback_data='prefs/timezone/auto_setup')
				],
					[
					InlineKeyboardButton(text='Ручное управление',
					callback_data='prefs/timezone/manual_setup')
					],
					[
					InlineKeyboardButton(text='Удалить мой часовой пояс',
					callback_data='prefs/timezone/remove')
					],
					[
					InlineKeyboardButton(text='Вернуться в основное меню',
					callback_data='prefs/main_menu')
					]])

				try:
					query.edit_message_text(text=inspect.cleandoc(text),
						reply_markup=keyboard,
						parse_mode='Markdown')
				except telegram.error.BadRequest:
					pass

				query.answer('Изменения успешны')

			elif input_data[2] == 'manual_setup':
				current_time_zone = load_time_zone_status(DATA_DIR,
					chat,
					readable=True)

				text = f'''Ручная настройка времени
							
				Используйте кнопки снизу чтобы настроить время

				Ваш часовой пояс сейчас *UTC{current_time_zone}*
				'''

				keyboard = InlineKeyboardMarkup(inline_keyboard=[[
					InlineKeyboardButton(text='-5 часов',
					callback_data='prefs/timezone/set/-5h'),
					InlineKeyboardButton(text='-1 час',
					callback_data='prefs/timezone/set/-1h'),
					InlineKeyboardButton(text='+1 час',
					callback_data='prefs/timezone/set/+1h'),
					InlineKeyboardButton(text='+5 часов',
					callback_data='prefs/timezone/set/+5h')
				],
					[
					InlineKeyboardButton(text='-15 минут',
					callback_data='prefs/timezone/set/-15m'),
					InlineKeyboardButton(text='+15 минут',
					callback_data='prefs/timezone/set/+15m'),
					],
					[
					InlineKeyboardButton(text='Вернуться в меню',
					callback_data='prefs/main_menu')
					]])

				try:
					query.edit_message_text(text=inspect.cleandoc(text),
						parse_mode='Markdown',
						reply_markup=keyboard,
						disable_web_page_preview=True)
				except telegram.error.BadRequest:
					pass

			elif input_data[2] == 'cancel':
				message_text = f'''
				Изменение настроек чата

				Раздел включает в себя
				Часотота уведомлений
				Часовой пояс

				'''
				try:
					sent_message = context.bot.send_message(chat,
						inspect.cleandoc(message_text),
						parse_mode='Markdown',
						reply_markup=ReplyKeyboardRemove(remove_keyboard=True))
				except telegram.error.RetryAfter as error:
					retry_after(error.retry_after)

					sent_message = context.bot.send_message(chat,
						inspect.cleandoc(message_text),
						parse_mode='Markdown',
						reply_markup=ReplyKeyboardRemove(remove_keyboard=True))

				try:
					context.bot.deleteMessage(sent_message.chat.id,
						sent_message.message_id)
				except telegram.error.RetryAfter as error:
					retry_after(error.retry_after)

					context.bot.deleteMessage(sent_message.chat.id,
						sent_message.message_id)

				keyboard = InlineKeyboardMarkup(inline_keyboard=[[
					InlineKeyboardButton(text='Настройки уведомлений',
					callback_data='prefs/notifs')
				],
					[
					InlineKeyboardButton(text='Вернуться в меню',
					callback_data='notify/main_menu/refresh_text')
					]])

				try:
					sent_message = context.bot.send_message(chat,
						inspect.cleandoc(message_text),
						parse_mode='Markdown',
						reply_markup=keyboard)
				except telegram.error.RetryAfter as error:
					retry_after(error.retry_after)

					sent_message = context.bot.send_message(chat,
						inspect.cleandoc(message_text),
						parse_mode='Markdown',
						reply_markup=keyboard)

				query.answer(text='Операция отменена')

			elif input_data[2] == 'set':
				update_time_zone_value(DATA_DIR, chat, input_data[3])
				current_time_zone = load_time_zone_status(data_dir=DATA_DIR,
					chat=chat,
					readable=True)

				text = f'''Ручная настройка времени
							
				Используйте кнопки снизу чтобы настроить время

				Ваш часовой пояс сейчас *UTC{current_time_zone}*
				'''

				keyboard = InlineKeyboardMarkup(inline_keyboard=[[
					InlineKeyboardButton(text='-5 часов',
					callback_data='prefs/timezone/set/-5h'),
					InlineKeyboardButton(text='-1 час',
					callback_data='prefs/timezone/set/-1h'),
					InlineKeyboardButton(text='+1 час',
					callback_data='prefs/timezone/set/+1h'),
					InlineKeyboardButton(text='+5 час',
					callback_data='prefs/timezone/set/+5h')
				],
					[
					InlineKeyboardButton(text='-15 minutes',
					callback_data='prefs/timezone/set/-15m'),
					InlineKeyboardButton(text='+15 minutes',
					callback_data='prefs/timezone/set/+15m'),
					],
					[
					InlineKeyboardButton(text='Вернуться в меню',
					callback_data='prefs/main_menu')
					]])

				query.answer(text=f'Часовой пояс изменен на UTC{current_time_zone}')
				try:
					query.edit_message_text(text=inspect.cleandoc(text),
						reply_markup=keyboard,
						parse_mode='Markdown',
						disable_web_page_preview=True)
				except telegram.error.BadRequest:
					pass

			elif input_data[2] == 'auto_setup':
				query.answer('Часовой пояс определен автоматически')
				text = '''Автоматическое определение времени

				Вы можете в любой момент удалить часовой пояс. Данные о местоположении не сохраняются

				Чтобы выбрать часовой пояс, ответьте на это сообщение текущей локацией.
				'''

				context.bot.delete_message(msg.chat.id, msg.message_id)
				sent_message = context.bot.send_message(chat,
					text=inspect.cleandoc(text),
					reply_markup=ForceReply(selective=True),
					parse_mode='Markdown')

				time_zone_setup_chats[chat] = [
					sent_message.message_id, from_id
				]

			elif input_data[2] == 'remove':
				remove_time_zone_information(DATA_DIR, chat)
				query.answer(
					'Ваша информация удалена с сервера',
					show_alert=True)

				text = f'''Установка времени

				Выберите нужное
				🌎 *Автоматическое определение*

				🕹 *Ручная настройка*

				Your current time zone is *UTC{load_time_zone_status(data_dir=DATA_DIR, chat=chat, readable=True)}*
				'''

				keyboard = InlineKeyboardMarkup(inline_keyboard=[[
					InlineKeyboardButton(text='Автоматическое определение',
					callback_data='prefs/timezone/auto_setup')
				],
					[
					InlineKeyboardButton(text='Ручная настройка',
					callback_data='prefs/timezone/manual_setup')
					],
					[
					InlineKeyboardButton(text='Удалить мой часовой пояс',
					callback_data='prefs/timezone/remove')
					],
					[
					InlineKeyboardButton(text='Вернуться в меню',
					callback_data='prefs/main_menu')
					]])

				try:
					query.edit_message_text(text=inspect.cleandoc(text),
						reply_markup=keyboard,
						parse_mode='Markdown')
				except:
					pass

	elif input_data[0] == 'stats':
		if input_data[1] == 'refresh':
			new_text = generate_statistics_message()
			if msg.text == new_text.replace('*', ''):
				query.answer(text='Обновлено')
				return

			keyboard = InlineKeyboardMarkup(inline_keyboard=[[
				InlineKeyboardButton(text='Обновить',
				callback_data='stats/refresh')
			]])

			try:
				query.edit_message_text(text=new_text,
					reply_markup=keyboard,
					parse_mode='Markdown',
					disable_web_page_preview=True)
			except telegram.error.BadRequest:
				pass

			query.answer(text='Обновить')

	if input_data[0] != 'stats':
		update_stats_db(stats_update={'commands': 1}, db_path=DATA_DIR)




def location_handler(update, context):
	chat = update.message.chat

	if update.message.reply_to_message is not None:
		if chat.id not in time_zone_setup_chats.keys():
			return

		if (update.message.from_user.id == time_zone_setup_chats[chat.id][1]
			and update.message.reply_to_message.message_id
			== time_zone_setup_chats[chat.id][0]):

			context.bot.deleteMessage(chat.id,
				time_zone_setup_chats[chat.id][0])

			try:
				context.bot.deleteMessage(chat.id, update.message.message_id)
			except:
				pass

			latitude = update.message.location.latitude
			longitude = update.message.location.longitude

			timezone_str = TimezoneFinder().timezone_at(lng=longitude,
				lat=latitude)
			timezone = pytz.timezone(timezone_str)

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

			new_text = f'''Настройки времени

			Часовой пояс установлен. Ваш часовой пояс *UTC{utc_offset_str} ({timezone_str})*
			'''

			keyboard = InlineKeyboardMarkup(inline_keyboard=[[
				InlineKeyboardButton(text='Вернуться в меню',
				callback_data='prefs/main_menu')
			]])

			context.bot.send_message(chat.id,
				text=inspect.cleandoc(new_text),
				reply_markup=keyboard,
				parse_mode='Markdown')

			update_time_zone_string(DATA_DIR, chat.id, timezone_str)


def timer_handle(update, context, command, chat, user):
	command = command.strip('/')
	chat = str(chat)

	if '@' in command:
		command = command.split('@')[0]

	now_called = time.time()

	try:
		cooldown = command_cooldowns['command_timers'][command]
	except KeyError:
		command_cooldowns['command_timers'][command] = 1
		cooldown = command_cooldowns['command_timers'][command]

	if cooldown <= -1:
		return False

	if chat not in chat_command_calls:
		chat_command_calls[chat] = {}

	if command not in chat_command_calls[chat]:
		chat_command_calls[chat][command] = 0

	try:
		last_called = chat_command_calls[chat][command]
	except KeyError:
		if chat not in chat_command_calls:
			chat_command_calls[chat] = {}

		if command not in chat_command_calls[chat]:
			chat_command_calls[chat][command] = 0

		last_called = chat_command_calls[chat][command]

	if last_called == 0:
		chat_command_calls[chat][command] = now_called

	else:
		time_since = now_called - last_called

		if time_since > cooldown:
			chat_command_calls[chat][command] = now_called
		else:
			if rd.exists('spammers'):
				if rd.hexists('spammers', user):
					offenses = int(rd.hget('spammers', user))
				else:
					offenses = None
			else:
				offenses = None

			if offenses is not None:
				offenses += 1
				if offenses >= 10:
					bot_running = time.time() - STARTUP_TIME
					if bot_running > 60:
						ignored_users.add(user)
						context.bot.send_message(chat,
							'Пожалуйства, не спамьте, иначе будете добавлены в черный список',
							parse_mode='Markdown',
							reply_to_message_id=update.message.message_id)
					else:
						run_time_ = int(time.time()) - STARTUP_TIME

						rd.hset('spammers', user, 0)

					return False

				rd.hset('spammers', user, offenses)
			else:
				rd.hset('spammers', user, 1)

			return False

	return True


def start(update, context):
	if not command_pre_handler(update, context, True):
		return

	try:
		command_ = update.message.text.strip().split(' ')[0]
	except:
		command_ = '/start'

	reply_msg = f'''Добрый день, это бот космический исследований @SpaceResearchBot

	Лист команд
	/notify добавление уведомлений
	/next показывает следующий полет
	/schedule расписание полетов на 5 дней

	'''

	chat_id = update.message.chat.id
	context.bot.send_message(chat_id,
		inspect.cleandoc(reply_msg),
		parse_mode='Markdown',
		disable_web_page_preview=True)
	try:
		if command_ == '/start':
			notify(update, context)
	except:
		notify(update, context)

	update_stats_db(stats_update={'commands': 1}, db_path=DATA_DIR)


def name_from_provider_id(lsp_id):
	conn = sqlite3.connect(os.path.join(DATA_DIR, 'launchbot-data.db'))
	cursor_ = conn.cursor()

	cursor_.execute("SELECT lsp_name FROM launches WHERE lsp_id = ?",
		(lsp_id, ))
	query_return = cursor_.fetchall()

	if len(query_return) != 0:
		return query_return[0][0]

	return lsp_id


def notify(update, context):
	if not command_pre_handler(update, context, False):
		returnъ
	message_text = '''
	Настройки уведомлений

	Здесь вы можете поменять свой часовой пояс, как часто вам приходят уведомления и от кого.

	🔔 = включено 
	🔕 = выключено
	'''

	chat = update.message.chat.id
	lsp_set = set()
	for cc_lsp_set in provider_by_cc.values():
		lsp_set = lsp_set.union(cc_lsp_set)

	notification_statuses = get_user_notifications_status(db_dir=DATA_DIR,
		chat=chat,
		provider_set=lsp_set,
		provider_name_map=provider_name_map)

	disabled_count = 1 if 0 in notification_statuses.values() else 0

	toggle_text = 'включить' if disabled_count != 0 else 'выключить'
	global_text = f' нажмите чтобы {toggle_text} все'

	keyboard = InlineKeyboardMarkup(inline_keyboard=[[
		InlineKeyboardButton(text=global_text,
		callback_data='notify/toggle/all/all')
	],
		[
		InlineKeyboardButton(text='🇪🇺 Европа', callback_data='notify/list/EU'),
		InlineKeyboardButton(text='🇺🇸 США', callback_data='notify/list/USA')
		],
		[
		InlineKeyboardButton(text='🇷🇺 Россия',callback_data='notify/list/RUS'),
		InlineKeyboardButton(text='🇨🇳 Китай', callback_data='notify/list/CHN')
		],
		[
		InlineKeyboardButton(text='🇮🇳 Индия', callback_data='notify/list/IND'),
		InlineKeyboardButton(text='🇯🇵 Япония', callback_data='notify/list/JPN')
		],
		[
		InlineKeyboardButton(text='🇹🇼 Тайвань',
		callback_data=f'notify/list/TWN')
		],
		[
		InlineKeyboardButton(text='Редактирование настроек',
		callback_data='prefs/main_menu')
		],
		[
		InlineKeyboardButton(text='Сохранить и выйти',
		callback_data='notify/done')
		]])

	try:
		context.bot.send_message(chat,
			inspect.cleandoc(message_text),
			parse_mode='Markdown',
			reply_markup=keyboard)
	except telegram.error.RetryAfter as error:
		retry_after(error.retry_after)
		context.bot.send_message(chat,
			inspect.cleandoc(message_text),
			parse_mode='Markdown',
			reply_markup=keyboard)

	update_stats_db(stats_update={'commands': 1}, db_path=DATA_DIR)



def generate_changelog():
	changelog = f'Логи'

	return changelog


def generate_schedule_message(call_type: str, chat: str):

	conn = sqlite3.connect(os.path.join(DATA_DIR, 'launchbot-data.db'))
	conn.row_factory = sqlite3.Row
	cursor_ = conn.cursor()

	cursor_.execute('SELECT * FROM launches WHERE net_unix >= ?',
		(int(time.time()), ))

	query_return = [dict(row) for row in cursor_.fetchall()]
	query_return.sort(key=lambda tup: tup['net_unix'])

	conn.close()

	month_map = {
		1: 'Январь',
		2: 'Февраль',
		3: 'Март',
		4: 'Апрель',
		5: 'Май',
		6: 'Июнь',
		7: 'Июль',
		8: 'Август',
		9: 'Сентябрь',
		10: 'Октябрь',
		11: 'Ноябрь',
		12: 'Декабрь'
	}
	providers_short = {
		"RL": "Rocket Lab",
		"RFSA": "Роскосмос",
		"VO": "Virgin Orbit",
		"Astra Space": "Astra",
		"FA": "Firefly",
		"MOD_RUS": "Российские военные силы",
		"NGIS": "Northrop Gr."
	}
	vehicle_map = {"Falcon 9 Block 5": "Falcon 9", "Firefly Alpha": "Alpha"}

	user_tz_offset = 3600 * load_time_zone_status(
		DATA_DIR, chat, readable=False)

	sched_dict = {}
	for i, row in enumerate(query_return):
		launch_unix = datetime.datetime.utcfromtimestamp(row['net_unix'] +
			user_tz_offset)

		if len(row['lsp_name']) <= len('Arianespace'):
			provider = row['lsp_name']
		else:
			if row['lsp_short'] not in (None, ''):
				provider = row['lsp_short']
			else:
				provider = row['lsp_name']

		try:
			mission = row['name'].split('|')[1].strip()
		except IndexError:
			mission = row['name'].strip()

		if mission[0] == ' ':
			mission = mission[1:]

		if '(' in mission:
			mission = mission[0:mission.index('(')]

		if provider in providers_short.keys():
			provider = providers_short[provider]

		vehicle = row['rocket_name'].split('/')[0]

		country_code = row['lsp_country_code']
		flag = map_country_code_to_flag(country_code)

		if vehicle in vehicle_map.keys():
			vehicle = vehicle_map[vehicle]

		provider = short_monospaced_text(provider)
		vehicle = short_monospaced_text(vehicle)
		mission = short_monospaced_text(mission)

		flt_str = flag if flag is not None else ''

		go_status = row['status_state']
		if go_status == 'GO':
			flt_str += '🟢'
		elif go_status == 'TBC':
			flt_str += '🟡'
		elif go_status == 'TBD':
			flt_str += '🔴'
		elif go_status == 'HOLD':
			flt_str += '⏸'
		elif go_status == 'FLYING':
			flt_str += '🚀'

		if call_type == 'vehicle':
			flt_str += f' {provider} {vehicle}'

		elif call_type == 'mission':
			flt_str += f' {mission}'

		utc_str = f'{launch_unix.year}-{launch_unix.month}-{launch_unix.day}'

		if utc_str not in sched_dict:
			if len(sched_dict.keys()) == 5:
				break

			sched_dict[utc_str] = [flt_str]
		else:
			sched_dict[utc_str].append(flt_str)

	schedule_msg, i = '', 0
	for key, val in sched_dict.items():
		if i != 0:
			schedule_msg += '\n\n'
		ymd_split = key.split('-')
		

		launch_date = datetime.datetime.strptime(key, '%Y-%m-%d')

		today = datetime.datetime.utcfromtimestamp(time.time() +
			user_tz_offset)
		time_delta = abs(launch_date - today)

		if (launch_date.day, launch_date.month) == (today.day, today.month):
			eta_days = 'сегодня'

		else:
			if launch_date.month == today.month:
				if launch_date.day - today.day == 1:
					eta_days = 'завтра'
				else:
					eta_days = f'через {launch_date.day - today.day} дней'
			else:
				sec_time = time_delta.seconds + time_delta.days * 3600 * 24
				days = math.floor(sec_time / (3600 * 24))
				hours = (sec_time / (3600) - math.floor(sec_time /
					(3600 * 24)) * 24)

				if today.hour + hours >= 24:
					days += 1

				eta_days = f'через {days+1} дней'

		eta_days = provider = ' '.join("`{}`".format(word)
			for word in eta_days.split(' '))

		schedule_msg += f'*{month_map[int(ymd_split[1])]} {ymd_split[2]}* {eta_days}\n'
		for mission, j in zip(val, range(len(val))):
			if j != 0:
				schedule_msg += '\n'

			schedule_msg += mission

			if j == 2 and len(val) > 3:
				upcoming_flight_count = 'полет' if len(
					val) - 3 == 1 else 'полеты'
				schedule_msg += f'\n+ {len(val)-3} больше {upcoming_flight_count}'
				break

		i += 1

	schedule_msg = reconstruct_message_for_markdown(schedule_msg)

	utc_offset = load_time_zone_status(DATA_DIR, chat, readable=True)

	header = 'Расписание запусков на 5 дней\n'
	header_note = f'Для деталей используйте /next@{BOT_USERNAME}.'
	footer_note = '\n\n🟢 = Точное время\n🟡 = Не подтвержденное время\n🔴 = Неизвестное время'

	footer = f'_{reconstruct_message_for_markdown(footer_note)}_'
	header_info = f'_{reconstruct_message_for_markdown(header_note)}\n\n_'

	schedule_msg = header + header_info + schedule_msg + footer

	switch_text = 'Ракеты' if call_type == 'mission' else 'Миссии'

	inline_keyboard = []
	inline_keyboard.append([
		InlineKeyboardButton(text='Обновить',
		callback_data=f'schedule/refresh/{call_type}'),
		InlineKeyboardButton(text=switch_text,
		callback_data=
		f"schedule/{'mission' if call_type == 'vehicle' else 'vehicle'}")
	])

	keyboard = InlineKeyboardMarkup(inline_keyboard=inline_keyboard)

	return schedule_msg, keyboard


def flight_schedule(update, context):
	if not command_pre_handler(update, context, False):
		return
	chat_id = update.message.chat.id

	schedule_msg, keyboard = generate_schedule_message(call_type='vehicle',
		chat=chat_id)

	try:
		context.bot.send_message(chat_id,
			schedule_msg,
			reply_markup=keyboard,
			parse_mode='MarkdownV2')

	except telegram.error.Unauthorized as error:
		clean_chats_db(db_path=DATA_DIR, chat=chat_id)

	except telegram.error.RetryAfter as error:
		retry_after(error.retry_after)

		context.bot.send_message(chat_id,
			schedule_msg,
			reply_markup=keyboard,
			parse_mode='MarkdownV2')

	except telegram.error.TimedOut as error:
		time.sleep(1)
		flight_schedule(update, context)

	update_stats_db(stats_update={'commands': 1}, db_path=DATA_DIR)


def generate_next_flight_message(chat, current_index: int):
	def cached_response():
		try:
			max_index = int(rd.get(f'next-{chat}-maxindex'))
		except TypeError:
			generate_next_flight_message(chat, current_index)
			return

		if max_index > 1:
			inline_keyboard = [[]]
			back, fwd = False, False

			if current_index != 0:
				back = True
				inline_keyboard[0].append(
					InlineKeyboardButton(text='Предыдущий',
					callback_data=f'next_flight/prev/{current_index}'))

			inline_keyboard[0].append(
				InlineKeyboardButton(text='Обновить',
				callback_data=f'next_flight/refresh/{current_index}'))

			if current_index + 1 < max_index:
				fwd = True
				inline_keyboard[0].append(
					InlineKeyboardButton(text='Следующий',
					callback_data=f'next_flight/next/{current_index}'))

			if len(inline_keyboard[0]) == 1:
				if fwd:
					inline_keyboard = [[]]
					inline_keyboard[0].append(
						InlineKeyboardButton(text='Обновить',
						callback_data=f'next_flight/refresh/0'))
					inline_keyboard[0].append(
						InlineKeyboardButton(text='Следующий',
						callback_data=f'next_flight/next/{current_index}'))
				elif back:
					inline_keyboard = [([
						InlineKeyboardButton(text='Предыдущий',
						callback_data=f'next_flight/prev/{current_index}')
					])]
					inline_keyboard.append([
						(InlineKeyboardButton(text='Первый',
						callback_data=f'next_flight/prev/1'))
					])

			keyboard = InlineKeyboardMarkup(inline_keyboard=inline_keyboard)

		elif max_index == 1:
			inline_keyboard = []
			inline_keyboard.append([
				InlineKeyboardButton(text='Обновить',
				callback_data=f'next_flight/prev/1')
			])

			keyboard = InlineKeyboardMarkup(inline_keyboard=inline_keyboard)

		launch_net = int(rd.get(f'next-{chat}-{current_index}-net'))
		eta = abs(int(time.time()) - launch_net)

		next_str = rd.get(f'next-{chat}-{current_index}')

		launch_status = rd.get(f'next-{chat}-{current_index}-status')
		if launch_status is not False:
			if launch_status in ('GO', 'TBC', 'TBD'):
				if launch_net < int(time.time()):
					eta_str = 'Ожидание изменений'
					next_str = next_str.replace('???ETASTR???',
						short_monospaced_text(eta_str))
				else:
					eta_str = time_delta_to_legible_eta(time_delta=eta,
						full_accuracy=True)
					next_str = next_str.replace('???ETASTR???',
						short_monospaced_text(eta_str))
			else:
				if launch_status == 'HOLD':
					t_prefx, eta_str = '', 'Полет приостановлен'

				elif launch_status == 'FLYING':
					t_prefx, eta_str = '', 'Ракета в полете'

				else:
					t_prefx, eta_str = '⚠️', 'ошибка'

				next_str = next_str.replace(
					'???ETASTR???',
					f'{t_prefx} {short_monospaced_text(eta_str)}')
		else:
			eta_str = time_delta_to_legible_eta(time_delta=eta,
				full_accuracy=True)
			next_str = next_str.replace('???ETASTR???',
				short_monospaced_text(eta_str))
		return inspect.cleandoc(next_str), keyboard

	if rd.exists(f'next-{chat}-{current_index}'):
		return cached_response()

	conn = sqlite3.connect(os.path.join(DATA_DIR, 'launchbot-data.db'))
	conn.row_factory = sqlite3.Row
	cursor_ = conn.cursor()

	cursor_.execute(
		'SELECT name FROM sqlite_master WHERE type = ? AND name = ?',
		('table', 'chats'))
	if len(cursor_.fetchall()) == 0:
		create_chats_db(db_path=DATA_DIR, cursor=cursor_)
		conn.commit()

	cursor_.execute('''SELECT * FROM chats WHERE chat = ?''', (chat, ))

	query_return = [dict(row) for row in cursor_.fetchall()]

	all_flag = False

	if len(query_return) == 0:
		cmd, user_notif_enabled = 'all', False
		enabled, disabled = [], []
	else:
		user_notif_enabled = None
		cmd = None

		chat_row = query_return[0]

		enabled, disabled = [], []

		try:
			enabled = chat_row['enabled_notifications'].split(',')
		except AttributeError:
			enabled = []

		try:
			disabled = chat_row['disabled_notifications'].split(',')
		except AttributeError:
			disabled = []

		if '' in enabled:
			enabled.remove('')

		if '' in disabled:
			disabled.remove('')

		if 'All' in enabled:
			all_flag, user_notif_enabled = True, True
			if len(disabled) == 0:
				cmd = 'all'
		else:
			all_flag = False
			user_notif_enabled = True

		if len(enabled) == 0:
			user_notif_enabled = False
	if len(enabled) == 0:
		cmd = 'all'
	today_unix = int(time.time())
	flying_net_window = int(time.time()) - 3600

	if cmd == 'all':
		cursor_.execute(
			'''
			SELECT * FROM launches WHERE net_unix >= ? OR launched = 0 
			AND status_state = ? OR status_state = ? AND net_unix >= ?''',
			(today_unix, 'HOLD', 'FLYING', flying_net_window))

		query_return = cursor_.fetchall()

	elif cmd is None:
		if all_flag:
			if len(disabled) > 0:
				disabled_str = ''
				for enum, lsp in enumerate(disabled):
					disabled_str += f"'{lsp}'"
					if enum < len(disabled) - 1:
						disabled_str += ','

				query_str = f'''SELECT * FROM launches WHERE net_unix >= ? AND lsp_name NOT IN ({disabled_str})
				AND lsp_short NOT IN ({disabled_str})'''

				cursor_.execute(query_str, (today_unix, ))
				query_return = cursor_.fetchall()

			else:
				cursor_.execute('SELECT * FROM launches WHERE net_unix >= ?',
					(today_unix, ))
				query_return = cursor_.fetchall()
		else:
			enabled_str = ''
			for enum, lsp in enumerate(enabled):
				enabled_str += f"'{lsp}'"
				if enum < len(enabled) - 1:
					enabled_str += ','

			query_str = f'''SELECT * FROM launches WHERE net_unix >= ? AND lsp_name IN ({enabled_str})
			OR net_unix >= ? AND lsp_short IN ({enabled_str})'''

			cursor_.execute(query_str, (today_unix, today_unix))
			query_return = cursor_.fetchall()

	conn.close()

	max_index = len(query_return)
	if max_index > 0:
		query_return.sort(key=lambda tup: tup[3])
		try:
			launch = dict(query_return[current_index])
		except Exception as error:
			launch = dict(query_return[0])
	else:
		msg_text = 'Полетов не найдено. Попробуйте изменить настройки поиска'
		inline_keyboard = []
		inline_keyboard.append([
			InlineKeyboardButton(text='Добавить уведомления',
			callback_data='notify/main_menu/refresh_text')
		])
		inline_keyboard.append([
			InlineKeyboardButton(text='Искать все полеты',
			callback_data='next_flight/refresh/0/all')
		])
		keyboard = InlineKeyboardMarkup(inline_keyboard=inline_keyboard)

		return reconstruct_message_for_markdown(msg_text), keyboard

	try:
		launch_name = launch['name'].split('|')[1].strip()
	except IndexError:
		launch_name = launch['name'].strip()

	if len(launch['lsp_name']) > len('Galactic Energy'):
		if launch['lsp_id'] in LSP_IDs.keys():
			lsp_name = LSP_IDs[launch['lsp_id']][0]
		else:
			if launch['lsp_short'] not in (None, ''):
				lsp_name = launch['lsp_short']
			else:
				lsp_name = launch['lsp_name']
	else:
		lsp_name = launch['lsp_name']

	if launch['lsp_id'] in LSP_IDs.keys():
		lsp_flag = LSP_IDs[launch['lsp_id']][1]
	else:
		lsp_flag = map_country_code_to_flag(launch['lsp_country_code'])

	if 'LC-' not in launch['pad_name']:
		launch['pad_name'] = launch['pad_name'].replace(
			'Space Launch Complex ', 'SLC-')
		launch['pad_name'] = launch['pad_name'].replace(
			'Launch Complex ', 'LC-')

	if 'air launch' in launch['pad_name'].lower():
		launch['pad_name'] = 'Достигло орбиты'

	launch_site = launch['location_name'].split(',')[0].strip()
	location_flag = map_country_code_to_flag(launch['location_country_code'])
	location = f'{launch["pad_name"]}, {launch_site} {location_flag}'

	eta = abs(int(time.time()) - launch['net_unix'])

	if launch['status_state'] in ('GO', 'TBD', 'TBC'):
		t_prefx, eta_str = '⏰', time_delta_to_legible_eta(time_delta=eta,
			full_accuracy=True)
	elif launch['status_state'] == 'HOLD':
		t_prefx, eta_str = '⏸', 'Отправка приостановлена'
	elif launch['status_state'] == 'FLYING':
		t_prefx, eta_str = '🚀', 'Ракета в полете'
	else:
		t_prefx, eta_str = '⚠️', 'Ошибка'

	user_tz_offset = 3600 * load_time_zone_status(
		DATA_DIR, chat, readable=False)

	launch_datetime = datetime.datetime.utcfromtimestamp(launch['net_unix'] +
		user_tz_offset)
	if launch_datetime.minute < 10:
		min_time = f'0{launch_datetime.minute}'
	else:
		min_time = launch_datetime.minute

	launch_time = f'{launch_datetime.hour}:{min_time}'

	date_str = timestamp_to_legible_date_string(timestamp=launch['net_unix'] +
		user_tz_offset,
		use_utc=True)

	if launch['status_state'] in ('GO', 'TBC', 'FLYING'):
		readable_utc_offset = load_time_zone_status(data_dir=DATA_DIR,
			chat=chat,
			readable=True)

		if launch['status_state'] in ('GO', 'FLYING'):
			time_str = f'{date_str}, {launch_time} UTC{readable_utc_offset}'
		else:
			time_str = f'{date_str}, NET {launch_time} UTC{readable_utc_offset}'
	else:
		if launch['status_state'] == 'TBD':
			time_str = f'не раньше {date_str}'
		else:
			if launch['status_state'] == 'HOLD':
				time_str = 'Ожидание новой даты отправки'

	mission_type = launch['mission_type'].capitalize(
	) if launch['mission_type'] is not None else 'Неизвестная цель'

	orbit_map = {
		'Sub': 'Суб-орбитальная',
		'VLEO': 'Очень низкая орбита',
		'LEO': 'Низкая орбита',
		'SSO': 'Солнечно-синхронная орбита',
		'MEO': 'Среднаяя орбита',
		'GTO': 'Геопереходная орбита',
		'Direct-GEO': 'Геостационарная орбита (выход)',
		'GSO': 'Геостационарная орбита',
		'LO': 'Лунная орбита'
	}

	try:
		if launch['mission_orbit_abbrev'] in orbit_map.keys():
			orbit_str = orbit_map[launch['mission_orbit_abbrev']]
		else:
			orbit_str = launch['mission_orbit'] if launch[
				'mission_orbit_abbrev'] is not None else 'Неизвестно'
			if 'Starlink' in launch_name:
				orbit_str = 'низкая орбита'
	except:
		orbit_str = 'Неизвестная орбита'

	if launch['spacecraft_crew_count'] not in (None, 0):
		if 'Dragon' in launch['spacecraft_name']:
			spacecraft_info = f'''
			Информация о Dragon
			Команда {short_monospaced_text("👨‍🚀" * launch["spacecraft_crew_count"])}
			Капсула {short_monospaced_text(launch["spacecraft_sn"])}
			'''
		else:
			spacecraft_info = None
	else:
		spacecraft_info = None

	if isinstance(launch['launcher_landing_attempt'], str):
		multiple_boosters = bool(';;' in launch['launcher_landing_attempt'])
	else:
		multiple_boosters = False

	landing_loc_map = {
		'OCISLY': 'Атлантический океан',
		'JRTI': 'Атлантический океан',
		'ASLOG': 'Тихий океан',
		'LZ-1': 'CCAFS RTLS',
		'LZ-2': 'CCAFS RTLS',
		'LZ-4': 'VAFB RTLS',
		'ATL': 'Столкновение',
		'PAC': 'Столкновение'
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

			reuse_str = f'первый полёт {core_str}'

		if launch['launcher_landing_location'] in landing_loc_map.keys():
			landing_type = landing_loc_map[launch['launcher_landing_location']]
			landing_str = f"{launch['launcher_landing_location']} ({landing_type})"
		else:
			landing_type = launch['landing_type']
			landing_str = f"{launch['launcher_landing_location']} ({landing_type})"

		if lsp_name == 'SpaceX' and 'Starship' in launch["rocket_name"]:
			location = f'SpaceX Южный Техас, Boca Chica {location_flag}'
			recovery_str = f'''
			Информация о ракете
			Корабль {short_monospaced_text(reuse_str)}
			Посадка {short_monospaced_text(landing_str)}
			'''
		else:
			recovery_str = f'''
			Информация о ракете
			Ядро {short_monospaced_text(reuse_str)}
			Посадка {short_monospaced_text(landing_str)}
			'''

	elif multiple_boosters:
		booster_indices = {'core': None, 'boosters': []}
		for enum, stage_type in enumerate(
			launch['launcher_stage_type'].split(';;')):
			if stage_type.lower() == 'core':
				booster_indices['core'] = enum
			elif stage_type.lower() == 'strap-on booster':
				booster_indices['boosters'].append(enum)

		recovery_str = '''\nИнформация о ракете'''

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

				reuse_str = f'{core_str} ({suffixed_readable_int(int(reuse_count))} полет)'
			else:
				if lsp_name == 'SpaceX' and core_str[0:2] == 'B1':
					core_str += '.1'

				reuse_str = f'первый полет {core_str}'

			landing_loc = launch['launcher_landing_location'].split(';;')[idx]
			if landing_loc in landing_loc_map.keys():
				landing_type = landing_loc_map[landing_loc]
				if landing_loc in ('ATL', 'PAC'):
					landing_loc = 'Океан'

				landing_str = f"{landing_loc} ({landing_type})"
			else:
				landing_type = launch['landing_type'].split(';;')[idx]
				landing_str = f"{landing_loc} ({landing_type})"

			if is_core:
				booster_str = f'''
				*Ядро* {short_monospaced_text(reuse_str)}
				*↪* {short_monospaced_text(landing_str)}'''
			else:
				booster_str = f'''
				*Ускорители* {short_monospaced_text(reuse_str)}
				*↪* {short_monospaced_text(landing_str)}'''

			recovery_str += booster_str

		recovery_str += '\n'

	else:
		recovery_str = None
	
	info_str = launch['mission_description']
	if info_str in (None, ''):
		info_str = 'Нет никакой информации'



	if user_notif_enabled:
		user_notif_states = chat_row['notify_time_pref']
		if '1' in user_notif_states:
			notify_str = 'Вы будуте получать уведомления'
		else:
			notify_str = 'Вы не будуте получать уведомления'
			notify_str += f'\nЧтобы включить /notify@{BOT_USERNAME}'
	else:
		notify_str = 'Вы не будуте получать уведомления'
		notify_str += f'\nЧтобы включить /notify@{BOT_USERNAME}'

	next_str = f'''
	Следующий полёт | {short_monospaced_text(lsp_name)}
	Миссия {short_monospaced_text(launch_name)}
	Ракета {short_monospaced_text(launch["rocket_name"])}
	Локация {short_monospaced_text(location)}

	{short_monospaced_text(time_str)}
	???ETASTR???

	Информация
	Тип {short_monospaced_text(mission_type)}
	Орбита {short_monospaced_text(orbit_str)}
	'''

	if spacecraft_info is not None:
		next_str += spacecraft_info

	if recovery_str is not None:
		next_str += recovery_str


	next_str += f'''
	{info_str}

	{notify_str}
	'''

	next_str = next_str.replace('\t', '')

	if max_index > 1:
		inline_keyboard = [[]]
		back, fwd = False, False

		if current_index != 0:
			back = True
			inline_keyboard[0].append(
				InlineKeyboardButton(text='Предыдущий',
				callback_data=f'next_flight/prev/{current_index}'))

		inline_keyboard[0].append(
			InlineKeyboardButton(text='Обновить',
			callback_data=f'next_flight/refresh/{current_index}'))

		if current_index + 1 < max_index:
			fwd = True
			inline_keyboard[0].append(
				InlineKeyboardButton(text='Следующий',
				callback_data=f'next_flight/next/{current_index}'))

		if len(inline_keyboard[0]) == 1:
			if fwd:
				inline_keyboard = [[]]
				inline_keyboard[0].append(
					InlineKeyboardButton(text='Обновить',
					callback_data=f'next_flight/refresh/0'))
				inline_keyboard[0].append(
					InlineKeyboardButton(text='Вперед',
					callback_data=f'next_flight/next/{current_index}'))
			elif back:
				inline_keyboard = [([
					InlineKeyboardButton(text='Назад',
					callback_data=f'next_flight/prev/{current_index}')
				])]
				inline_keyboard.append([(InlineKeyboardButton(text='На первый',
					callback_data=f'next_flight/prev/1'))])

		keyboard = InlineKeyboardMarkup(inline_keyboard=inline_keyboard)

	elif max_index == 1:
		inline_keyboard = []
		inline_keyboard.append([
			InlineKeyboardButton(text='Обновить',
			callback_data=f'next_flight/prev/1')
		])

		keyboard = InlineKeyboardMarkup(inline_keyboard=inline_keyboard)

	next_str = reconstruct_message_for_markdown(next_str)

	if rd.exists('next-api-update'):
		to_next_update = int(float(rd.get('next-api-update'))) - int(
			time.time()) + 30
	else:
		to_next_update = 30 * 60

	if to_next_update < 0:
		to_next_update = 60

	rd.setex(f'next-{chat}-maxindex',
		datetime.timedelta(seconds=to_next_update),
		value=max_index)
	rd.setex(f'next-{chat}-{current_index}',
		datetime.timedelta(seconds=to_next_update),
		value=next_str)
	rd.setex(f'next-{chat}-{current_index}-net',
		datetime.timedelta(seconds=to_next_update),
		value=launch['net_unix'])
	rd.setex(f'next-{chat}-{current_index}-status',
		datetime.timedelta(seconds=to_next_update),
		value=launch['status_state'])

	next_str = next_str.replace('???ETASTR???',
		f'{t_prefx} {short_monospaced_text(eta_str)}')

	return inspect.cleandoc(next_str), keyboard


def next_flight(update, context):
	if not command_pre_handler(update, context, False):
		return

	chat_id = update.message.chat.id

	message, keyboard = generate_next_flight_message(chat_id, 0)

	try:
		context.bot.send_message(chat_id,
			message,
			reply_markup=keyboard,
			parse_mode='MarkdownV2')

	except telegram.error.Unauthorized as error:
		clean_chats_db(db_path=DATA_DIR, chat=chat_id)

	except telegram.error.RetryAfter as error:
		retry_after(error.retry_after)

		context.bot.send_message(chat_id,
			message,
			reply_markup=keyboard,
			parse_mode='MarkdownV2')

	except telegram.error.TimedOut as error:
		next_flight(update, context)

	update_stats_db(stats_update={'commands': 1}, db_path=DATA_DIR)

def update_token(data_dir: str):
	config_ = load_config(data_dir)
	token_input = str(input('Enter the bot token for LaunchBot: '))
	while ':' not in token_input:
		token_input = str(input('Enter the bot token for launchbot: '))

	config_['bot_token'] = token_input

	store_config(config_, data_dir)

	time.sleep(2)
	print('Token update successful!\n')


def sigterm_handler(signal, frame):
	running_sec = int(time.time() - STARTUP_TIME)
	time_running = time_delta_to_legible_eta(time_delta=running_sec,
		full_accuracy=True)
	sys.exit(0)


def apscheduler_event_listener(event):
	if event.exception:
		if not os.path.isdir("error-logs"):
			os.mkdir("error-logs")

		efname = os.path.join("error-logs", f"error-{int(time.time())}.log")
		with open(efname, "w") as ef:
			ef.write(
				f"ERROR EXPERIENCED AT {datetime.datetime.now().ctime()}\n\n")
			ef.write(f"EXCEPTION: {event.exception}")
			ef.write("TRACEBACK FOLLOWS:")
			ef.write(event.traceback)

			ef.write(f"EXCEPTION VARS: {vars(event)}")


if __name__ == '__main__':
	DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
	OLD_DATA_DIR = os.path.join(os.path.dirname(__file__), 'launchbot')

	if not os.path.isdir(DATA_DIR) and os.path.isdir(OLD_DATA_DIR):
		os.rename(OLD_DATA_DIR, DATA_DIR)

	STARTUP_TIME = time.time()

	parser = argparse.ArgumentParser('spaceresearchbot.py')

	parser.add_argument('-start',
		dest='start',
		help='Starts the bot',
		action='store_true')
	parser.add_argument('-debug',
		dest='debug',
		help='Disables the activity indicator',
		action='store_true')
	parser.add_argument('--new-bot-token',
		dest='update_token',
		help='Set a new bot token',
		action='store_true')
	parser.add_argument('--force-api-update',
		dest='force_api_update',
		help='Force an API update on startup',
		action='store_true')
	parser.add_argument('--disable-api-updates',
		dest='api_updates_disabled',
		help='Disables API update scheduler',
		action='store_true')

	parser.set_defaults(start=False, newBotToken=False, debug=False)
	args = parser.parse_args()

	if args.update_token:
		update_token(data_dir=DATA_DIR)

	if not args.start:
		sys.exit(
			'Нет стартовой команды'
		)

	config = load_config(data_dir=DATA_DIR)

	try:
		local_api_conf = config['local_api_server']
	except KeyError:
		config = repair_config(data_dir=DATA_DIR)
		local_api_conf = config['local_api_server']

	if (local_api_conf['enabled'], local_api_conf['logged_out']) == (True,
		True):
		api_url = local_api_conf['address']
		updater = Updater(config['bot_token'],
			use_context=True,
			base_url=api_url)
	else:
		updater = Updater(config['bot_token'], use_context=True)

		if local_api_conf['enabled'] is True:
			if updater.bot.log_out():
				config['local_api_server']['logged_out'] = True
				store_config(config_json=config, data_dir=DATA_DIR)
				updater.base_url = config['local_api_server']['address']
			else:
				sys.exit('Выход...')
		elif local_api_conf['enabled'] is False and local_api_conf[
			'logged_out'] is True:
			if updater.bot.log_out():
				config['local_api_server']['logged_out'] = False
				store_config(config_json=config, data_dir=DATA_DIR)
				updater = Updater(config['bot_token'], use_context=True)

			else:
				sys.exit('Выход...')

	try:
		bot_specs = updater.bot.getMe()
	except telegram.error.Unauthorized:
		sys.exit(
			'Невозможно запустить бота'
		)


	BOT_USERNAME = bot_specs.username
	BOT_ID = bot_specs.id
	OWNER = config['owner']
	VALID_COMMANDS = {
		'/start', '/help', '/next', '/notify', '/statistics', '/schedule',
		'/feedback'
	}
	alt_commands = set()
	for command in VALID_COMMANDS:
		alt_commands.add(f'{command}@{BOT_USERNAME.lower()}')
	VALID_COMMANDS = tuple(VALID_COMMANDS.union(alt_commands))

	global provider_by_cc
	provider_by_cc = {
		'USA': {
		'NASA', 'SpaceX', 'ULA', 'Rocket Lab Ltd', 'Blue Origin',
		'Astra Space', 'Virgin Orbit', 'Firefly Aerospace', 'Northrop Grumman',
		'International Launch Services'
		},
		'EU': {'Arianespace', 'Eurockot', 'Starsem SA'},
		'CHN': {'CASC', 'ExPace', 'iSpace', 'Galactic Energy'},
		'RUS': {
		'KhSC', 'ISC Kosmotras', 'Космические войска', 'Eurockot',
		'Sea Launch', 'Land Launch', 'Starsem SA',
		'International Launch Services', 'Роскосмос'
		},
		'IND': {'ISRO', 'Antrix Corporation'},
		'JPN':
		{'JAXA', 'Mitsubishi Heavy Industries', 'Interstellar Technologies'},
		'TWN': {'TiSPACE'}
	}
	global time_zone_setup_chats
	time_zone_setup_chats = {}
	global LSP_IDs
	LSP_IDs = {
		121: ['SpaceX', '🇺🇸'],
		147: ['Rocket Lab', '🇺🇸'],
		265: ['Firefly', '🇺🇸'],
		141: ['Blue Origin', '🇺🇸'],
		99: ['Northrop Grumman', '🇺🇸'],
		115: ['Arianespace', '🇪🇺'],
		124: ['ULA', '🇺🇸'],
		98: ['Mitsubishi Heavy Industries', '🇯🇵'],
		1002: ['Interstellar Tech.', '🇯🇵'],
		88: ['CASC', '🇨🇳'],
		190: ['Antrix Corporation', '🇮🇳'],
		122: ['Sea Launch', '🇷🇺'],
		118: ['ILS', '🇺🇸🇷🇺'],
		193: ['Eurockot', '🇪🇺🇷🇺'],
		119: ['ISC Kosmotras', '🇷🇺🇺🇦🇰🇿'],
		123: ['Starsem', '🇪🇺🇷🇺'],
		194: ['ExPace', '🇨🇳'],
		63: ['Роскосмос', '🇷🇺'],
		175: ['Миноборона', '🇷🇺']
	}
	global provider_name_map
	provider_name_map = {
		'Rocket Lab': 'Rocket Lab Ltd',
		'Northrop Grumman': 'Northrop Grumman Innovation Systems',
		'ROSCOSMOS': 'Russian Federal Space Agency (ROSCOSMOS)',
		'Ministry of Defence': 'Ministry of Defence of the Russian Federation'
	}

	global command_cooldowns, chat_command_calls, spammers, ignored_users
	command_cooldowns, chat_command_calls = {}, {}
	spammers, ignored_users = set(), set()

	command_cooldowns['command_timers'] = {}
	for command in VALID_COMMANDS:
		command_cooldowns['command_timers'][command.replace('/', '')] = 1

	global feedback_message_IDs
	feedback_message_IDs = set()

	signal.signal(signal.SIGTERM, sigterm_handler)

	log = os.path.join(DATA_DIR, 'log-file.log')


	scheduler = BackgroundScheduler()
	scheduler.start()

	scheduler.add_listener(apscheduler_event_listener, EVENT_JOB_ERROR)

	dispatcher = updater.dispatcher

	dispatcher.add_handler(CommandHandler(command='notify', callback=notify))
	dispatcher.add_handler(CommandHandler(command='next',
		callback=next_flight))
	dispatcher.add_handler(
		CommandHandler(command='schedule', callback=flight_schedule))
	dispatcher.add_handler(
		CommandHandler(command=('start', 'help'), callback=start))
	dispatcher.add_handler(
		CallbackQueryHandler(callback=callback_handler, run_async=True))
	dispatcher.add_handler(
		MessageHandler(Filters.reply & Filters.location & ~Filters.forwarded
		& ~Filters.command,
		callback=location_handler))
	dispatcher.add_handler(
		MessageHandler(Filters.status_update, callback=generic_update_handler))

	if OWNER != 0:
		dispatcher.add_handler(
			CommandHandler(command='debug',
			callback=admin_handler,
			filters=Filters.chat(OWNER)))

	updater.start_polling()

	if args.force_api_update:
		api_update_on_restart()

	if not args.api_updates_disabled:
		api_call_scheduler(db_path=DATA_DIR,
			ignore_60=False,
			scheduler=scheduler,
			bot_username=BOT_USERNAME,
			bot=updater.bot)

	if OWNER != 0:
		try:
			updater.bot.send_message(OWNER,
				f'Бот запущен',
				parse_mode='Markdown')
		except telegram.error.Unauthorized:
			pass

	try:
		if not args.debug:
			try:
				cursor.hide()
			except AttributeError:
				pass

			while True:
				for char in ('⠷', '⠯', '⠟', '⠻', '⠽', '⠾'):
					sys.stdout.write('%s\r' %
						'   Подключено к телеграмму, выход через ctrl + c.')
					sys.stdout.write('\033[92m%s\r\033[0m' % char)
					sys.stdout.flush()
					time.sleep(0.1)
		else:
			while True:
				time.sleep(10)

	except KeyboardInterrupt:
		updater.stop()

		try:
			cursor.show()
		except AttributeError:
			pass

		scheduler.shutdown()

	except Exception as error:
		updater.bot.send_message(OWNER,
			f'Произошло отключение {error}')
