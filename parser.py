# -*- coding: utf-8 -*-

from grab import Grab, error
import psycopg2
import sys
import datetime

class Parser:

	connection = None # соединение с БД
	cursor = None # курсор для взаимодействия с БД
	grabObject = None # объект grab
	tblName = None # название таблицы
	counter = None

	def __init__(self, dataBaseName, dbUsername, hostname, userPass, tableName):

		try:

			# Устанавливаем соединение с БД
			self.connection = psycopg2.connect(dbname = dataBaseName, user = dbUsername, host = hostname, password = userPass)

			# Создаем курсор и проверяем наличие таблицы
			self.cursor = self.connection.cursor()
			self.cursor.execute("SELECT * FROM information_schema.tables WHERE table_name = %s", (tableName, ))

			if self.cursor.rowcount == 0:
				print u'Таблица %s не существует' % tableName
				sys.exit(1)

			self.tblName = tableName

		except (psycopg2.DatabaseError) as dbError:
			print 'Ошибка подключения к базе', dbError
			sys.exit(1)

		# Настраиваем grab
		self.grabObject = Grab(log_file = 'out.html')
		self.grabObject.setup(url = 'http://egr.gov.by/egrn/index.jsp?content=Find', connect_timeout = 30, timeout = 30)


	def loadPage(self, counter):
		# Загружаем страницу

		self.counter = counter
		try:
			postParameters = dict(
					ngrn = counter,
					vname = '',
					fplace = 'all',
					ftype = 'in',
					fmax = '20',
					txtInput = '', )

			self.grabObject.setup(post = postParameters)
			self.grabObject.request()

		except(error.GrabError) as gError:
			print 'Ошибка в модуле grab ', gError
			raise KeyboardInterrupt


	def parseAndPush(self):
		# Парсинг страницы и занесение данных в БД

		dataList = []

		try:
			dataInTable = self.grabObject.doc.select('//body/div[contains(@id, "content")]/div/table/tr')[1] # Поиск таблицы 

			# Извлечение данных из таблицы
			dataList.append( dataInTable.select('.//td')[1].text() ) # Регистрационный номер

			# Название юридического лица / имя индивидуального предпринимателя 
			if (dataInTable.select('.//td/br').exists() == True):
				dataList.append( dataInTable.select('.//td/br')[1].html()[12:] )
			else:
				dataList.append( dataInTable.select('.//td')[2].text() )

			dataList.append( dataInTable.select('.//td')[3].text() ) # Регистрирующий орган
			dataList.append( datetime.datetime.strptime(dataInTable.select('.//td')[4].text(), "%d.%m.%Y").date() ) # Дата регистрации
			dataList.append( dataInTable.select('.//td')[5].text() ) # Статус

			exclusion_date = dataInTable.select('.//td')[6].text() # Дата исключения
			if (exclusion_date == '&nbsp'):
				dataList.append(None)
			else:
				dataList.append( datetime.datetime.strptime(exclusion_date, "%d.%m.%Y") )

			# запись в БД
			self.cursor.execute("""INSERT INTO register VALUES (%s, %s, %s, %s, %s, %s)""", dataList)
			self.connection.commit()

			return self.counter + 1

		except (IndexError):
			# Срабатывает при ненахождении html таблицы, то есть нет записи с таким регистрационным номером 
			return self.counter + 1

		except (psycopg2.IntegrityError):
		# Если запись с таким номером уже найдена, то ищем запись с наибольшим номером и начинаем с следующего номера

			self.connection.rollback()
			SQL = "SELECT MAX(reg_number) FROM %s" % self.tblName
			self.cursor.execute(SQL)

			self.counter = self.cursor.fetchone()[0] + 1
			self.loadPage( self.counter )
			self.parseAndPush()

			return self.counter + 1

	def __del__(self):

		if self.connection:
			self.connection.close()