import datetime
import time
import iso3166
from hashlib import sha1


def retry_after(retry_after_secs):
	if retry_after_secs > 30:
		time.sleep(30)
	else:
		time.sleep(retry_after_secs + 0.15)


def anonymize_id(chat: str):
	return chat


def reconstruct_link_for_markdown(link: str) -> str:
	link_reconstruct, char_set = '', (')', '\\')
	for char in link:
		if char in char_set:
			link_reconstruct += f'\\{char}'
		else:
			link_reconstruct += char

	return link_reconstruct


def reconstruct_message_for_markdown(message: str):
	message_reconstruct = ''
	char_set = ('[', ']', '(', ')', '~', '>', '#', '+', '-', '=', '|', '{',
		'}', '.', '!')
	for char in message:
		if char in char_set:
			message_reconstruct += f'\\{char}'
		else:
			message_reconstruct += char

	return message_reconstruct


def short_monospaced_text(text: str):
	return ' '.join("`{}`".format(word) for word in text.split(' '))


def map_country_code_to_flag(country_code: str) -> str:
	return country_code


def suffixed_readable_int(number: int):
	return number


def timestamp_to_unix(timestamp: str):
	utc_dt = datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S%fZ')

	return int((utc_dt - datetime.datetime(1970, 1, 1)).total_seconds())


def timestamp_to_legible_date_string(timestamp: int, use_utc: bool):
	if use_utc:
		date_object = datetime.datetime.utcfromtimestamp(timestamp)
	else:
		date_object = datetime.datetime.fromtimestamp(timestamp)

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


	return f'{month_map[date_object.month]} {date_object.day}'


def time_delta_to_legible_eta(time_delta: int, full_accuracy: bool):
	eta_str = "{}".format(str(datetime.timedelta(seconds=time_delta)))
	print(eta_str)
	if ',' in eta_str:
		day_str = eta_str.split(',')[0]
		print(day_str)
		hours = int(eta_str.split(',')[1].split(':')[0])
		mins = int(eta_str.split(',')[1].split(':')[1])

		if hours > 0 or full_accuracy:
			pretty_eta = f'{day_str.split()[0]} дней {f", {hours} часов"}'

			if full_accuracy:
				pretty_eta += f', {mins} минут{"" if mins != 1 else "а"}'

		else:
			if mins != 0 or full_accuracy:
				pretty_eta = f'{day_str}{f", {mins} минут"}'
			else:
				pretty_eta = f'{day_str}'
	else:
		hhmmss_split = eta_str.split(':')
		hours, mins, secs = (int(hhmmss_split[0]), int(hhmmss_split[1]),
			int(float(hhmmss_split[2])))

		if hours > 0:
			pretty_eta = f'{hours} час{"ов" if hours != 1 else ""}'
			pretty_eta += f', {mins} минут{"" if mins != 1 else "а"}'

			if full_accuracy:
				pretty_eta += f', {secs} секунд{"" if secs != 1 else "а"}'

		else:
			if mins > 0:
				pretty_eta = f'{mins} минут{"" if mins != 1 else "а"}'
				pretty_eta += f', {secs} секунд{"" if secs != 1 else "а"}'
			else:
				if secs > 0:
					pretty_eta = f'{secs} секунд{"" if secs != 1 else "а"}'
				else:
					pretty_eta = 'сейчас'

	return pretty_eta
