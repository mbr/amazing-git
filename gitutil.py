#!/usr/bin/env python
# coding=utf8

import sys
import os
import re

import logbook

log = logbook.Logger('gitutil')

class GitRemoteHandler(object):
	"""A standalone git-remote-helper handler.

	Parses input from git and translates it into method calls on an instance,
	also handles the capabilities command.

	To use, subclass from GitRemoteHandler and call the run() method. The handler
	will read each command and call the corresponding git_ method, i.e. a push
	command will be handled by a method called git_push. Any arguments space separated
	arguments are split and passed on.

	The handler also reads the commandline arguments and supplies two attributes,
	remote_name, containing the name of the remote, and remote_address, containing
	the remote URI.

	See http://www.kernel.org/pub/software/scm/git/docs/git-remote-helpers.html or
	git-remote-helpers (1) for details on git-remote-helpers."""

	supported_options = []
	"""A list of options supported by the remote handler. The "option" command allows
	the client to set options, supported options are available through the options
	attribute, unsupported ones will be rejected."""
	def __init__(self):
		self.args = sys.argv[1:]

		self.remote_name = None
		self.remote_address = None
		if 2 == len(self.args):
			self.remote_address = self.args[1]
			if self.args[1] != self.args[0]: self.remote_name = self.args[0]

		self.options = {}
		self._log = logbook.Logger('gitutil.GitRemoteHandler')

	def handle_command(self, line):
		"""Dispatch command.

		Called by run(), upon reading a command (line), call the corresponding
		function.

		Tries to flush after a command returns."""
		args = line.split(' ')
		command = args.pop(0)

		self._log.debug('on stdin: %s' % line)
		if not hasattr(self, 'git_' + command): raise AttributeError('git requested an unsupported command: %s' % command)
		getattr(self, 'git_' + command)(*args)
		try:
			sys.stdout.flush()
		except IOError:
			# git closed the connection
			pass

	def git_capabilities(self):
		"""Capabilities command.

		Handles the "capabilities" command of git, every function prefixed with "git_" is assumed
		to be a supported command and sent with the leading "git_" stripped."""
		caps = []
		for name in dir(self):
			if 'git_capabilities' == name: continue
			attr = getattr(self,name)
			if name.startswith('git_') and callable(attr):
				caps.append('*' + name[4:] if hasattr(attr, 'git_required') else name[4:])

		self._log.debug('sending capabilities: %s' % caps)
		for c in caps: print c

		# end with a blank line
		print

	def git_option(self, name, value):
		"""Handle git options.

		Options set by the git client are stored in the options attribute. Options not found
		in supported_options are rejected with an "unsupported" reply."""
		if name in self.supported_options:
			self.options[name] = value
			self._log.debug('option %s: %s' % (name, value))
			print "ok"
		else:
			self._log.debug('option %s unsupported' % name)
			print "unsupported"

	def run(self):
		"""Run, waiting for input.

		Blocks and reads commands, until stdin is closed, EOF is encountered or
		an empty line is sent."""
		self._log.debug('spawned process: %d' % os.getpid())
		self._log.debug('remote name: %s' % self.remote_name)
		self._log.debug('remote address: %s' % self.remote_address)
		while not sys.stdin.closed:
			line = sys.stdin.readline()
			if '' == line: break # EOF
			if '' == line.strip(): break # empty line ends as well
			try:
				self.handle_command(line.rstrip(os.linesep))
			except Exception, e:
				self._log.exception(e)
				print >>sys.stderr, os.path.basename(sys.argv[0]) + ':', e
				break


s3_url_exp = 's3://(?:(?P<key>[^:@]+)(?::(?P<secret>[^@]*))?@)?(?P<bucket>[^:@]+)(?::(?P<prefix>[^@]*))?$'
"""Regular expression used for matching S3 URLs.

s3 bucket URLs have the following format::

	s3://access_key:secret_key@bucket_name:some_prefix

The access_key, secret_key parts are optional. Thus it is possible to specify an URL without
any secret information on the commandline::

	s3://access_key@some_bucket:/myprefix

The shortest s3 URL is just a bucket with no prefix::

	s3://mahbukkit
"""


s3_url_re = re.compile(s3_url_exp)
"""Compiled version of s3_url_exp."""


def parse_s3_url(url):
	"""Parse url with s3_url_exp. If the url does not match, raise an Exception."""
	m = s3_url_re.match(url)
	if not m: raise Exception('Not a valid S3 URL: %s' % url)
	return m.groupdict()
