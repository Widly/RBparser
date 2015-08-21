# coding: utf-8

import cPickle as pickle
from parser import Parser
import sys
import signal

counter = 0 # Счетчик регистрационных номеров
serializedObject = {} # Объект сериализации
p = None # Класс для парсинга и записи в базу 

def signalToSerialization(signal = None, frame = None, serializationFile = 'ser.pickle'):
	# Сериализация

	if counter == 0:
		sys.exit(0)

	print u'\nПоследний обработанный регистрационный номер - %d' % (counter - 1)
	serializedObject['counter'] = counter
	with open(serializationFile, 'wb') as f:
		pickle.dump(serializedObject, f)

	sys.exit(0)


def deserialization(serializationFile = 'ser.pickle'):
	count = 0

	try:
		with open(serializationFile, 'rb') as f:
			serializedObject = pickle.load(f)

		# Проверка счетчика на соотвествие формату регистрационного номера
		count = serializedObject['counter']
		if count < 100000000 or count > 999999999 or (int(str(count)[1])  > 0 and int(str(count)[1]) < 9):
			print u'Неверный формат счетчика в ' + serializationFile
			count = 100000000

	except(IOError):
		count = 100000000

	finally:
		return count

# Нажатие ctrl-c запускает сериализацию, а затем завершает работу программы
signal.signal(signal.SIGINT, signalToSerialization)


# Разбор параметров командной строки
if len(sys.argv) == 2 and sys.argv[1] == '-help':
	print u'\nИнструкция по эксплуатации:'
	print u'Первый аргумент – имя базы данных в PSQL\nВторой аргумент – имя пользователя БД\nТретий аргумент – IP адрес хоста БД'
	print u'Четвертый аргумент – пароль пользователя БД\nПятый аргумент – имя таблицы в базе данных\n'
	print u'При необходимости, завершить работу программы можно комбинацией клавиш ctrl + C\n'
	sys.exit(0)

elif len(sys.argv) == 6:
	p = Parser(
			dataBaseName = sys.argv[1],
			dbUsername = sys.argv[2],
			hostname = sys.argv[3],
			userPass = sys.argv[4],
			tableName = sys.argv[5], )

else:
	print u'Неверный формат параметров запуска. Запустите скрипт с параметром -help для вывода инструкции'
	sys.exit(0)

# Десериализация
counter = deserialization()


try:
	# Код парсинга и добавления в БД
	firstDigit = int(str(counter)[0])
	secondDigit = int(str(counter)[1])

	for fd in range(firstDigit, 10):
		if secondDigit == 0:
			topLimit = fd * (10 ** 8) + (10 ** 7)

			while counter < topLimit:
				# Загрузка страницы
				p.loadPage(counter)

				# Парсинг и запись в БД
				counter = p.parseAndPush()

			counter = fd * (10 ** 8) + (9 * (10 ** 8))
			secondDigit = 9

		if secondDigit == 9:
			topLimit = (fd + 1) * (10 ** 8)

			while counter < topLimit:
				# Загрузка страницы
				p.loadPage(counter)

				# Парсинг и запись в БД
				counter = p.parseAndPush()

			secondDigit = 0

except (KeyboardInterrupt):
	signalToSerialization()

except (RuntimeError, ValueError) as error:
	signalToSerialization()
	raise error