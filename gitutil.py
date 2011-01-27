#!/usr/bin/env python
# coding=utf8

import sys
import os

import logbook

log = logbook.Logger('gitutil')

class GitRemoteHandler(object):
	supported_options = []
	def __init__(self):
		self.args = sys.argv[1:]

		self.remote_name = None
		self.remote_address = None
		if 2 == len(self.args):
			self.remote_address = self.args[1]
			if self.args[1] != self.args[0]: self.remote_name = self.args[0]

		self.options = {}

	def handle_command(self, line):
		args = line.split(' ')
		command = args.pop(0)

		log.debug('on stdin: %s' % line)
		if not hasattr(self, 'git_' + command): raise AttributeError('git requested an unsupported command: %s' % command)
		getattr(self, 'git_' + command)(*args)
		try:
			sys.stdout.flush()
		except IOError:
			# git closed the connection
			pass

	def git_capabilities(self):
		caps = []
		for name in dir(self):
			if 'git_capabilities' == name: continue
			attr = getattr(self,name)
			if name.startswith('git_') and callable(attr):
				caps.append('*' + name[4:] if hasattr(attr, 'git_required') else name[4:])

		log.debug('sending capabilities: %s' % caps)
		for c in caps: print c

		# end with a blank line
		print

	def run(self):
		log.debug('spawned process: %d' % os.getpid())
		log.debug('remote name: %s' % self.remote_name)
		log.debug('remote address: %s' % self.remote_address)
		while not sys.stdin.closed:
			line = sys.stdin.readline()
			if '' == line: break # EOF
			if '' == line.strip(): break # empty line ends as well
			try:
				self.handle_command(line.rstrip(os.linesep))
			except Exception, e:
				log.exception(e)
				print >>sys.stderr, os.path.basename(sys.argv[0]) + ':', e
				break
