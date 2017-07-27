import platform, os, time, re, ctypes, sys, subprocess
from subprocess import Popen, PIPE, STDOUT
from pprint import pprint

class Service(object):
	def __init__(self, name, folder, exec_command):
		self.name 			= name
		self.folder 		= folder
		self.exec_command 	= exec_command
		self.type 			= None
		self.command 		= None
		self.script 		= None
		self._check_admin()
		self._determine_type()
		self._determine_command()
		self._determine_script()


	def _system(self, command):
		x = Popen(command.split(' '), stdout=PIPE, stderr=STDOUT)
		stdout, nothing = x.communicate()

	def _check_admin(self):
		try:
			is_admin = os.getuid()==0
		except AttributeError:
			is_admin = ctypes.windll.shell32.IsUserAnAdmin()!=0
		if not is_admin:
			print '[!] error: need to be admin to enable "%s" as system service' % self.name
			sys.exit(1)

	def _determine_type(self):
		try:
			self._system('systemctl')
		except:
			pass
		else:
			self.type = 'systemctl'
			return

		try:
			self._system('initctl')
		except:
			pass
		else:
			self.type = 'initctl'
			return

		try:
			self._system('service')
		except:
			pass
		else:
			self.type = 'service'
			return

		

	def _determine_command(self):
		if self.type == 'initctl':
			self.command = 'initctl {{command}} %s' % self.name
		elif self.type == 'service':
			self.command = 'service %s {{command}}' % self.name
		elif self.type == 'systemctl':
			self.command = 'systemctl {{command}} %s' % self.name

	def _determine_script(self):
		if self.type == 'initctl' or self.type == 'service':
			self.script = ''
			self.script += 'description "WebAssistant Sensor"\n'
			self.script += 'start on runlevel [2345]\n'
			self.script += 'stop on runlevel [06]\n'
			self.script += 'respawn\n'
			self.script += '\nchdir %s/\n' % self.folder
			self.script += 'exec %s\n' % self.exec_command
		elif self.type == 'systemctl':
			self.script = ''
			self.script += 'Description="WebAssistant Sensor"\n'
			self.script += '[Service]\n'
			self.script += 'Type=simple\n'
			self.script += 'WorkingDirectory=%s\n' % self.folder
			self.script += 'ExecStart=%s\n'% self.exec_command
			self.script += 'Restart=always\n'
			self.script += '[Install]\n'
			self.script += 'WantedBy=multi-user.target\n'

	def exists(self):
		if self.type == 'initctl' or self.type == 'service':
			if os.path.exists('/etc/init/%s.conf' % self.name):
				return True
			return False
		elif self.type == 'systemctl':
			if os.path.exists('/etc/init.d/%s.service' % self.name):
				return True
			return False

	def enable(self):
		if self.type == 'initctl' or self.type == 'service':
			fp = open('/etc/init/%s.conf' % self.name, 'w')
			fp.write(self.script)
			fp.close()
		elif self.type == 'systemctl':
			fp = open('/etc/init.d/%s.service' % self.name, 'w')
			fp.write(self.script)
			fp.close()
			try:
				self._system('systemctl daemon-reload')
				self._system('systemctl enable /etc/init.d/%s.service' % self.name)
			except subprocess.CalledProcessError:
				self.type = 'service'
				self.enable()


	def disable(self):
		if self.type == 'initctl' or self.type == 'service':
			self._system('rm -f /etc/init/%s.conf' % self.name)
		elif self.type == 'systemctl':
			# self._system('/etc/init.d/%s.service' % self.name)
			# self._system('systemctl disable /etc/init.d/%s.service' % self.name)
			self._system('systemctl disable %s' % self.name)
			self._system('systemctl daemon-reload')


	def start(self):
		self._system(self.command.replace('{{command}}', 'start'))


	def stop(self):
		self._system(self.command.replace('{{command}}', 'stop'))


	def restart(self):
		self._system(self.command.replace('{{command}}', 'restart'))

	def status(self):
		command = self.command.replace('{{command}}', 'status')
		first_exec = os.popen(command).read()
		if self.type =='initctl' or self.type == 'service':
			if 'stop/waiting' in first_exec:
				return False
			elif 'start/running' in first_exec:
				return True
			else:
				print '[!] Warning: lib do not support this service!'
				print '	   Detail: ', first_exec
				return False
		elif self.type == 'systemctl':
			if 'inactive (dead)' in first_exec:
				return False
			elif 'active (running)' in first_exec:
				return True
			else:
				print '[!] Warning: lib do not support this service!'
				print '    Detail :', first_exec
				return False
		else:
			print '[!] Warning: lib do not support this platform!'
			print '    Detail :', platform.dist()
			return False


	def __str__(self):
		res = ''
		res += self.type + '\n'
		res += self.command + '\n'
		res += self.script + '\n'
		return res

if __name__ == '__main__':
	dir_name = os.path.dirname(os.path.abspath(__file__))
	file_name = os.path.join(dir_name, 'webchecker.py')
	print dir_name, file_name
	S = Service('webchecker', dir_name, '/usr/share/miniconda2/bin/python ' + file_name)
	S.enable()
	S.start()
	# S.stop()
	# pprint(S.__dict__)
	