# -*- coding: utf-8 -*-

from grab import Grab
from grab import error
import psycopg2
import sys
import datetime

class Parser:

	connection = None # соединение с БД
	cursor = None # курсор для взаимодействия с БД
	g = None # объект grab
	tblName = None # название таблицы
	counter = None

	def __init__(self, dataBaseName, dbUsername, hostname, userPass, tableName): # название базы, имя пользователя, хост, пароль, название таблицы 
		# Конструктор

		try:

			# Устанавливаем соединение с БД
			self.connection = psycopg2.connect(dbname = dataBaseName, user = dbUsername, host = hostname, password = userPass)

			# Создаем курсор и проверяем наличие таблицы
			self.cursor = self.connection.cursor()
			self.cursor.execute("SELECT * FROM information_schema.tables WHERE table_name=%s", (tableName, ))

			if bool(self.cursor.rowcount == False):
				print('table %s does not exist') % tableName
				sys.exit(1)

			self.tblName = tableName

		except (psycopg2.DatabaseError):
			print 'unable to connect to database'
			sys.exit(1)

		# Настраиваем grab
		self.g = Grab(log_file = 'out.html')
		self.g.setup(url = 'http://egr.gov.by/egrn/index.jsp?content=Find', connect_timeout = 30, timeout = 30)


	def loadPage(self, counter):
		# Загружаем страницу

		self.counter = counter
		try:
			self.g.setup(post = {'ngrn' : self.counter, 'vname' : '', 'fplace' : 'all', 'ftype' : 'in', 'fmax' : '20', 'txtInput' : ''})
			self.g.request()

		except(error.GrabError):
			print 'grab error'
			raise KeyboardInterrupt


	def parseAndPush(self):
		# Парсинг страницы и занесение данных в БД

		dataList = []

		try:
			dataInTable = self.g.doc.select('//body/div[contains(@id, "content")]/div/table/tr')[1] # Поиск таблицы 

			# Извлечение данных из таблицы
			dataList.append( dataInTable.select('.//td')[1].text() ) # Регистрационный номер

			# Название юридического лица / имя индивидуального предпринимателя 
			if (dataInTable.select('.//td/br').exists() == True):
				dataList.append( dataInTable.select('.//td/br')[1].html()[12:] )
			else:
				dataList.append( dataInTable.select('.//td')[2].text() )

			dataList.append( dataInTable.select('.//td')[3].text() ) # Регистрирующий орган
			dataList.append( datetime.datetime.strptime( dataInTable.select('.//td')[4].text(), "%d.%m.%Y").date() ) # Дата регистрации
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
		# Деструктор

		if self.connection:
			self.connection.close()