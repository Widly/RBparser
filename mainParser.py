# coding: utf-8

import cPickle as pickle
from parser import Parser
import sys
import signal

counter = 0 # Счетчик регистрационных номеров
serObj = {} # Объект сериализации

def signalToSerialization(signal = None, frame = None):
	# Сериализация

	if counter == 0:
		sys.exit(0)

	print counter
	serObj['counter'] = counter
	with open('ser.pickle', 'wb') as f:
		pickle.dump(serObj, f)

	sys.exit(0)

# Нажатие ctrl-c запускает сериализацию, а затем завершает работу программы
signal.signal(signal.SIGINT, signalToSerialization)

p = Parser('rbregister', 'rbuser', 'localhost', 'pass', 'register')

# Десериализация
try:
	with open('ser.pickle', 'rb') as f:
		serObj = pickle.load(f)

	# Проверка счетчика на соотвествие формату регистрационного номера
	counter = serObj['counter']
	if counter < 100000000 or counter > 999999999 or (int(str(counter)[1])  > 0 and int(str(counter)[1]) < 9):
		print 'wrong format of counter'
		counter = 100000000

except(IOError):
	counter = 100000000


try:
	# Код парсинга и добавления в БД
	firstDigit = counter // 100000000
	secondDigit = int(str(counter)[1])

	for fd in range(firstDigit, 10):
		if secondDigit == 0:
			topLimit = fd * 100000000 + 10000000

			while counter < topLimit:
				# Загрузка страницы
				p.loadPage(counter)

				# Парсинг и запись в БД
				counter = p.parseAndPush()

			counter = fd * 100000000 + 90000000
			secondDigit = 9

		if secondDigit == 9:
			topLimit = (fd + 1) * 100000000

			while counter < topLimit:
				# Загрузка страницы
				p.loadPage(counter)

				# Парсинг и запись в БД
				counter = p.parseAndPush()

			secondDigit = 0

except (KeyboardInterrupt, RuntimeError, ValueError):
	signalToSerialization()