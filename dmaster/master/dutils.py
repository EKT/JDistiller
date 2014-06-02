#   This file is part of JDistiller.
#
#   JDistiller is free software: you can redistribute it and/or modify
#       it under the terms of the GNU General Public License as published by
#       the Free Software Foundation, either version 3 of the License, or
#       (at your option) any later version.
#
#       JDistiller is distributed in the hope that it will be useful,
#       but WITHOUT ANY WARRANTY; without even the implied warranty of
#       MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#       GNU General Public License for more details.
#   You should have received a copy of the GNU General Public License
#   along with Foobar. If not, see <http://www.gnu.org/licenses/>.
#
# Copyright National Documentation Centre/NHRF 2012,2013,2014
#
# Software Contributors:
# version 2.0 Michael-Angelos Simos, Panagiotis Stathopoulos
# version 1.0 Chrysostomos Nanakos, Panagiotis Stathopoulos
# version 0.1 Panagiotis Stathopoulos

import logging
import logging.handlers
import smtplib
from email.MIMEText import MIMEText
import platform
import os
import sys
import time
import pyinotify
import zmq
from datetime import datetime
import platform

_conffile='distiller_master.conf'
## Read Configuration variables
config=ConfigParser.RawConfigParser()
config.read(_conffile)
_SENDER=config.get("EMAIL","Sender")
_SENDER_EMAIL= config.get("EMAIL","SenderEmail")



class ColorAttr:
	D_HEADER = '\033[95m'
        D_BLUE = '\033[94m'
        D_GREEN = '\033[92m'
        D_WARNING = '\033[93m'
        D_FAIL = '\033[91m'
	D_PURPLE = '\033[0;35m'
	D_YELLOW = '\033[0;33m'
	D_UYELLOW = '\033[4;33m'
	D_CYAN = '\033[0;36m'
	D_UBLACK = '\033[4;33m'
        D_ENDC = '\033[0m'

	def print_color_r(self,text,color):
		print "\r%s%s%s" % (color,text,self.D_ENDC)

class States:
	D_RUNNING = "RUNNING"
	D_DOWN = "DOWN"
	D_UNKNOWN = "UNKNOWN"
	D_ERROR = "ERROR"
	D_OK = "OK"
	D_NOK = "NOT_OK"
	D_FIREUP = "###FIRE_UP_CONVERTER###"
	D_GETSTATUS = "MASTER_GET_STATUS"
	D_GETQUEUES = "GET_QUEUES_SIZE"
	D_GETSTATS = "GET_NODE_STATS"
	D_GSTATUS = "GET_STATUS"
	D_GBUSY = "GET_BUSY"
	D_DBUSY = "BUSY"
	D_NBUSY = "NO_BUSY"
	D_TRY = "TRY"
	D_REGISTER = "REGISTER"
	D_REGISTERED = "REGISTERED"
	D_ERR_REGISTERED = "REG_ERROR"
	D_ACTIVE_NODES = "ACTIVE_NODES"
	D_NODES_STATUS = "NODES_STATUS"
	D_REG_NODES = "REGISTERED_NODES"
	D_MASTER_SERVICES = "MASTER_SERVICES"
	D_PING = "PING"
	D_PONG = "PONG"
	D_ACK_DIR = "ACK_DIR"

class Services:
	D_WORKER = "WORKER"
	D_QUEUE = "QUEUE"
	D_CONVERTER = "CONVERTER"
	D_REPOSITORIES = "REPOSITORIES"


class DistillerLogger:
        def __init__(self,log_file,logger_name):
                self.logger = logging.getLogger(logger_name)
                self.logger.setLevel(logging.DEBUG)
                #self.handler = logging.FileHandler(log_file)
                self.handler = logging.handlers.RotatingFileHandler(log_file,'a',10485760,8)
		self.handler.setLevel(logging.DEBUG)
                self.formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
		self.handler.setFormatter(self.formatter)
                self.logger.addHandler(self.handler)

class DistillerLoggerServer:
	def __init__(self,logserver,logger_name):
		self.logserver = logserver
		self.logger_name = logger_name
		self.connected = False
		self.connectLogServer()

	def connectLogServer(self):
		self.context = zmq.Context()
		self.logsock = self.context.socket(zmq.REQ)
		self.logsock.setsockopt(zmq.LINGER,0)
		try:
			self.logsock.connect(self.logserver)
			self.connected = True
		except Exception,e:
			self.context.destroy()

	def retryConnect(self):
		self.context = zmq.Context()
		self.logsock = self.context.socket(zmq.REQ)
		self.logsock.setsockopt(zmq.LINGER,0)
		try:
			self.logsock.connect(self.logserver)
			self.connected = True
		except Exception,e:
			self.context.destroy()
		

	def info(self,logmsg):
		if self.connected == True:
			final_msg = str(datetime.now())+" - "+platform.node()+" - "+self.logger_name+" - INFO - "+logmsg
			try:
				self.poller = zmq.Poller()
				self.poller.register(self.logsock,zmq.POLLOUT)
				if self.poller.poll(100):
					self.logsock.send(final_msg)
				else:
					del self.poller
					self.logsock.close()
					self.context.destroy()
					del self.context
					self.connected = False
					return False
				self.poller.register(self.logsock,zmq.POLLIN)
				if self.poller.poll(100):
					msg = self.logsock.recv()
				else:
					del self.poller
					self.logsock.close()
					self.context.destroy()
					del self.context
					self.connected = False
					return False
				return True	
			except Exception,e :
				self.connected = False
				self.logsock.close()
				self.context.destroy()
		elif self.connected == False:
			self.retryConnect()
			return False
	
	def error(self,logmsg):
		if self.connected == True:
			final_msg = str(datetime.now())+" - "+platform.node()+" - "+self.logger_name+" - ERROR - "+logmsg
			try:
				self.poller = zmq.Poller()
				self.poller.register(self.logsock,zmq.POLLOUT)
				if self.poller.poll(100):
					self.logsock.send(final_msg)
				else:
					del self.poller
					self.logsock.close()
					self.context.destroy()
					del self.context
					self.connected = False
					return False
				self.poller.register(self.logsock,zmq.POLLIN)
				if self.poller.poll(100):
					msg = self.logsock.recv()
				else:
					del self.poller
					self.logsock.close()
					self.context.destroy()
					del self.context
					self.connected = False
					return False
				return True	
			except Exception,e :
				self.connected = False
				self.logsock.close()
				self.context.destroy()
				del self.logsock; del self.context
		elif self.connected == False:
			self.retryConnect()
			return False
	
	def debug(self,logmsg):
		if self.connected == True:
			final_msg = str(datetime.now())+" - "+platform.node()+" - "+self.logger_name+" - DEBUG - "+logmsg
			try:
				self.poller = zmq.Poller()
				self.poller.register(self.logsock,zmq.POLLOUT)
				if self.poller.poll(100):
					self.logsock.send(final_msg)
				else:
					del self.poller
					self.logsock.close()
					self.context.destroy()
					del self.context
					self.connected = False
					return False
				self.poller.register(self.logsock,zmq.POLLIN)
				if self.poller.poll(100):
					msg = self.logsock.recv()
				else:
					del self.poller
					self.logsock.close()
					self.context.destroy()
					del self.context
					self.connected = False
					return False
				return True	
			except Exception,e :
				self.connected = False
				self.logsock.close()
				self.context.destroy()
				del self.logsock; del self.context
		elif self.connected == False:
			self.retryConnect()
			return False
	
	def warn(self,logmsg):
		if self.connected == True:
			final_msg = str(datetime.now())+" - "+platform.node()+" - "+self.logger_name+" - WARNING - "+logmsg
			try:
				self.poller = zmq.Poller()
				self.poller.register(self.logsock,zmq.POLLOUT)
				if self.poller.poll(100):
					self.logsock.send(final_msg)
				else:
					del self.poller
					self.logsock.close()
					self.context.destroy()
					del self.context
					self.connected = False
					return False
				self.poller.register(self.logsock,zmq.POLLIN)
				if self.poller.poll(100):
					msg = self.logsock.recv()
				else:
					del self.poller
					self.logsock.close()
					self.context.destroy()
					del self.context
					self.connected = False
					return False
				return True	
			except Exception,e :
				self.connected = False
				self.logsock.close()
				self.context.destroy()
				del self.logsock; del self.context
		elif self.connected == False:
			self.retryConnect()
			return False
	

class MailerStates:
	TYPE_OK = "EMAIL_OK"
	TYPE_ERROR = "EMAIL_ERROR"
	TYPE_WARNING = "EMAIL_WARN"


class DistillerMailer:
	def __init__(self,smtp_server,mailing_list,logfile):
		self.smtp_server = smtp_server
		self.mailing_list = mailing_list
		self.Log = DistillerLogger(logfile,"DistillerMailer")

	def sendmail_report(self,dirnames,email_type):
		text = str(platform.node())+" Export/Publish Report\n"
		text += "-"*len(text)+'\n'
		for dname in dirnames:
			if email_type == MailerStates.TYPE_OK:
				text += "Conversion succeeded for the directory: %s\n" % dname
			if email_type == MailerStates.TYPE_ERROR:
				text += "Conversion failed for the directory: %s\n" % dname
				
		msg = MIMEText(text)
		if email_type == MailerStates.TYPE_OK:
			msg['Subject'] = '[dPool Elastic Cluster] - Node:'+str(platform.node())+' - Successful Conversion'
		elif email_type == MailerStates.TYPE_ERROR:
			msg['Subject'] = '[dPool Elastic Cluster] - Node:'+str(platform.node())+' - Failed Conversion'
		elif email_type == MailerStates.TYPE_WARNING:
			msg['Subject'] = '[dPool Elastic Cluster] - Node:'+str(platform.node())+' - Warning'
		
		msg['From'] = _SENDER 
		try:
			s = smtplib.SMTP(self.smtp_server)
		except:
			self.Log.logger.error("Connection to SMTP Server %s failed." % self.smtp_server)

		for email_user in self.mailing_list:
			msg['To'] = email_user
			s.sendmail(_SENDER_EMAIL,[email_user],msg.as_string())
		s.quit()

	def sendmail_custom(self,subject,mailto,text):
				
		msg = MIMEText(text)
		msg['Subject'] = '[dPool Elastic Cluster] - Node:'+str(platform.node())+' - '+subject
		
		msg['From'] = _SENDER
		try:
			s = smtplib.SMTP(self.smtp_server)
		except:
			self.Log.logger.error("Connection to SMTP Server %s failed." % self.smtp_server)

		for email_user in mailto:
			msg['To'] = email_user
			s.sendmail(_SENDER_EMAIL,[email_user],msg.as_string())
		s.quit()

class DistillerTailEventHandler(pyinotify.ProcessEvent):
	def __init__(self,file_,callback_func):
		self.file_ = file_
		self.fh = open(self.file_,'r')
		self.fh.seek(0,2)
		self.callback = callback_func

	def process_IN_MODIFY(self,event):
		if self.file_ not in os.path.join(event.path,event.name):
			return 
		else:
			self.callback(self.fh.readline().rstrip())

	def process_IN_MOVE_SELF(self,event):
		print "The file moved. Continuing to read from that, until a new one is created.."
	
	def process_IN_CREATE(self,event):
		if self.file_ in os.path.join(event.path,event.name):
			self.fh = open(self.file_,'r')
			for line in self.fh.readlines():
				print line.rstrip()
			self.fh.seek(0,2)
		return
		

	
class DistillerTailLog(object):
	def __init__(self,tailed_file):
		self.check_file_validity(tailed_file)
		self.tailed_file = tailed_file
		self.callback = self.print_line

		self.watch_manager = pyinotify.WatchManager()
		self.directory_mask = pyinotify.IN_MODIFY | pyinotify.IN_DELETE | pyinotify.IN_MOVE_SELF | pyinotify.IN_CREATE
		self.notifier = pyinotify.Notifier(self.watch_manager,DistillerTailEventHandler(self.tailed_file,self.callback))
		self.watch_manager.add_watch(self.tailed_file[:self.tailed_file.rfind('/')],self.directory_mask)

	def follow(self):
			while True:
				try:
					self.notifier.process_events()
					if self.notifier.check_events():
						self.notifier.read_events()
				except KeyboardInterrupt:
					ColorAttr().print_color_r("Exiting...",ColorAttr.D_FAIL)
					break
	def coloured(self,s,color):
                        return '\033[%s%s\033[0m' % (color, s)
                        #return '\033[1;%s%s\033[1;m' % (color, s) #BOLD

	def print_line(self,line):
                        if line.find("- INFO -") > 0:
                                print self.coloured(line.strip(),"92m")
                        elif line.find("- WARNING -") > 0:
                                print self.coloured(line.strip(),"93m")
			elif line.find(" - ERROR -") > 0:
                                print self.coloured(line.strip(),"91m")
			elif line.find(" - DEBUG -") > 0:
                                print self.coloured(line.strip(),"94m")

	def register_callback(self,func):
		self.callback = func

	def check_file_validity(self,file_):
		if not os.access(file_,os.F_OK):
			raise DistillerTailError("File '%s' does not exist" % (file_))	
		if not os.access(file_,os.R_OK):
			raise DistillerTailError("File '%s' not readable" % (file_))	
		if os.path.isdir(file_):
			raise DistillerTailError("File '%s' is a directory" % (file_))

	def tail(self, n=1, bs=1024):
		f=open(self.tailed_file)
		f.seek(-1,2)
		l = 1-f.read(1).count('\n') # If file doesn't end in \n, count it anyway.
		B = f.tell()
		while n >= l and B > 0:
			block = min(bs, B)
			B -= block
			f.seek(B, 0)
			l += f.read(block).count('\n')
		f.seek(B, 0)
		l = min(l,n) # discard first (incomplete) line if l > n
		lines = f.readlines()[-l:]
		f.close()
		for line in lines:
			self.print_line(line)

class DistillerTailError(Exception):
	def __init__(self,msg):
		self.message = msg
	def __str__(self):
		return self.message
