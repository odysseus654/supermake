import socket, traceback, time, thread, urllib2, urllib, urlparse, sys, xml.sax, cookielib, json, os.path
import base, task
from base import Asset, Transform, AssetPlaceholder
from task import ThreadTask, TaskLaunchError

def formatFilesize(size):
	if size == 1:
		return str(size) + ' byte'
	if size < 1000:
		return str(size) + ' bytes'
	size = size / 1024.0
	if size < 10:
		return str(int(size*100)/100.0) + 'kb'
	if size < 100:
		return str(int(size*10)/10.0) + 'kb'
	if size < 1000:
		return str(int(size)) + 'kb'
	size = size / 1024.0
	if size < 10:
		return str(int(size*100)/100.0) + ' MB'
	if size < 100:
		return str(int(size*10)/10.0) + ' MB'
	if size < 1000:
		return str(int(size)) + ' MB'
	size = size / 1024.0
	if size < 10:
		return str(int(size*100)/100.0) + ' GB'
	if size < 100:
		return str(int(size*10)/10.0) + ' GB'
	if size < 1000:
		return str(int(size)) + ' GB'
	size = size / 1024.0
	if size < 10:
		return str(int(size*100)/100.0) + ' TB'
	if size < 100:
		return str(int(size*10)/10.0) + ' TB'
	return str(int(size)) + ' TB'

###############################################################################
class TivoServerListenerTransform(Transform):
	SPEC = base.TransformSpec()
	SPEC.produces = [AssetPlaceholder('TivoServer', {'id': base.TransformPlaceholder(), 'mediaKey': base.TransformPlaceholder() })]
	
	def __init__(self):
		Transform.__init__(self)
	def spec(self):
		return self.SPEC
	def newTask(self,env,input,output):
		return TivoServerListenerTask(self,env)
	def isRunning(self,tasks,input,output):
		for task in tasks:
			return True
		return False
	
class TivoServerListenerTask(ThreadTask):
	SOCKET_PORT    = 2190
	SERVER_STALE   = 120
	SERVER_EXPIRE  = 300
	SOCKET_TIMEOUT = 10

	def __init__(self, xform, env):
		ThreadTask.__init__(self)
		self.xform = xform
		self.tivoStatus = {}					# no lock necessary as it's only ever looked at by the worker thread
		self.env = env
	
	def name(self):
		return "Tivo Broadcast Listener"
	def stop(self):
		self.threadStatus = ThreadTask.thrCANCELLING
	def transform(self):
		return self.xform
		
	def serverSeen(self, id, attrs):
		if id in self.tivoStatus:
			self.tivoStatus[id]['lastSeen'] = time.time()
			if 'stale' in self.tivoStatus[id]:
				del self.tivoStatus[id]['stale']
				self.tivoStatus[id]['asset'].delFlag(Asset.flagSTALE)
			if self.tivoStatus[id]['attr'] != attrs:	# this only happens if something serious changed, like a version upgrade or the tivo rebooted or something
				self.tivoStatus[id]['asset'].resetAttrs(attrs)
		else:
			newServer = TivoServer(attrs)
			self.tivoStatus[id] = {'attr': attrs, 'lastSeen': time.time(), 'asset': newServer }
			if self.env is not None:
				self.env.declareAsset(newServer)

	def pruneServers(self):
		currentTime = time.time()
		for id in self.tivoStatus.keys():
			if currentTime - self.tivoStatus[id]['lastSeen'] > self.SERVER_STALE and 'stale' not in self.tivoStatus[id]:
				self.tivoStatus[id]['asset'].addFlag(Asset.flagSTALE)
				self.tivoStatus[id]['stale'] = True
			elif currentTime - self.tivoStatus[id]['lastSeen'] > self.SERVER_EXPIRE:
				self.tivoStatus[id]['asset'].close()
				if self.env is not None:
					self.env.undeclareAsset(self.tivoStatus[id]['asset'])
				del self.tivoStatus[id]

	def handleBcast(self, addr, body):
		attrs = { 'address': addr };
		for value in body.split("\n"):
			if value != "":
				keyvalue = value.split("=", 2);
				attrs[keyvalue[0].lower()] = keyvalue[1];
		if ('tivoconnect' in attrs) and attrs['tivoconnect']=='1':
			del attrs['tivoconnect']
			if 'method' in attrs:
				del attrs['method']
			if 'services' in attrs:
				serviceMap = {}
				for serviceStr in attrs['services'].split(","):
					split1 = serviceStr.split("/", 2)
					split2 = split1[0].split(":", 2)
					service = {}
					if len(split1) > 1:
						service['proto'] = split1[1]
					if len(split2) > 1:
						service['port'] = int(split2[1])
					serviceMap[split2[0]] = service
				attrs['services'] = serviceMap
			if ('identity' in attrs):
				id = attrs['identity']
				self.serverSeen(id, attrs)

	def run(self):
		s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
		s.settimeout(self.SOCKET_TIMEOUT)
		s.bind(('', self.SOCKET_PORT))
		while self.threadStatus == ThreadTask.thrRUNNING:
			try:
				self.pruneServers()
				message, address = s.recvfrom(8192)
				self.handleBcast(address[0], message)
			except socket.timeout:
				pass
		s.shutdown(socket.SHUT_RDWR)
		s.close()

###############################################################################
class SimpleXmlObject(object):
	def __init__(self):
		self.document = None
		self.stack = []
		self.currentContainer = None
		self.lightNode = None
		self.lightValue = None

	def pushToHeavyNode(self):
		assert(self.lightNode is not None and self.lightValue is None)
		self.stack = self.stack + [self.currentContainer]
		newContainer = {}
		self.pushIntoContainer(self.lightNode, newContainer)
		self.currentContainer = newContainer
		self.lightNode = None

	def pushLightNode(self):
		assert(self.lightNode is not None)
		self.pushIntoContainer(self.lightNode, self.lightValue)

	def pushIntoContainer(self, name, value):
		if name in self.currentContainer:
			if type(self.currentContainer[name]) == type([]):
				self.currentContainer[name] = self.currentContainer[name] + [value]
			else:
				self.currentContainer[name] = [self.currentContainer[name], value]
		else:
			self.currentContainer[name] = value

	def setDocumentLocator(self,locator):
		pass

	def startDocument(self):
		self.document = {}
		self.currentContainer = self.document

	def endDocument(self):
		pass

	def startElement(self, name, attrs):
		if self.lightNode is not None:	# umm, the parent isn't a light node
			self.pushToHeavyNode()
		assert(self.lightNode is None)
		self.lightNode = name
		self.lightValue = None

	def endElement(self, name):
		if self.lightNode is not None:	#this is a light node, let's push it as an attribute
			assert(self.lightNode == name)
			self.pushLightNode()
			self.lightNode = None
		else:
			self.currentContainer = self.stack[len(self.stack)-1]
			self.stack = self.stack[:-1]

	def characters(self, content):
		assert(self.lightNode is not None)
		if self.lightValue is None:
			self.lightValue = content
		else:
			self.lightValue = self.lightValue + content

	def startPrefixMapping(self, prefix, uri):
		pass
	def endPrefixMapping(self, prefix):
		pass
	def startElementNS(self, name, qname, attrs):
		pass
	def endElementNS(self, name, qname):
		pass
	def ignorableWhitespace(self, whitespace):
		pass
	def processingInstruction(self, target, data):
		pass
	def skippedEntity(self, name):
		pass

class TivoServerQuery(object):
	REQUEST_SIZE = 20
	
	def __init__(self, mediaKey):
		self.mediaKey = mediaKey
		self.opener = None

	def initOpener(self, addr):
		auth = urllib2.HTTPDigestAuthHandler()
		cookies = urllib2.HTTPCookieProcessor(cookielib.CookieJar())
		auth.add_password('TiVo DVR', addr, 'tivo', self.mediaKey)
		handlers = [auth, cookies]
		self.opener = urllib2.build_opener(*handlers)

	def crackUrl(self, url):
		parsed = urlparse.urlparse(url)
		args = urlparse.parse_qs(parsed.query)
		for arg in args:
			args[arg] = args[arg][0]
		cracked = { 'proto': parsed.scheme, 'host': parsed.hostname, 'args': args, 'path': parsed.path }
		if parsed.port is not None:
			cracked['port'] = parsed.port
		return cracked

	def assembleUrl(self, addr):
		proto = addr['proto']
		netloc = addr['host']
		if 'port' in addr:
			if proto == 'http' and addr['port'] == 80:
				pass
			elif proto == 'https' and addr['port'] == 443:
				pass
			else:
				netloc = netloc + ":%d" % addr['port']
		path = addr.get('path', '')
		args = urllib.urlencode(addr.get('args',''))
		return urlparse.urlunparse(urlparse.ParseResult(proto, netloc, path, '', args, ''))

	def openXmlPath(self, addr):
		addr = dict(addr) # make local copy
		if 'path' not in addr:
			addr['path'] = '/TiVoConnect'
		if 'args' not in addr:
			addr['args'] = {}
		addr['args']['Command'] = 'QueryContainer'
		addr['args']['ItemCount'] = self.REQUEST_SIZE
		addr['args']['Recurse'] = 'Yes'
		
		#retrieve the request
		try:
			url = self.openSimplePath(addr)
			obj = SimpleXmlObject()
			xml.sax.parseString(url.read(), obj)
#		except urllib2.HTTPError:
#			print "HTTP error: %d" % sys.exc_value.code
#			print sys.exc_value.info()
		except xml.sax.SAXParseException:
			return None
		return obj.document

	def openSimplePath(self, addr):
		if self.opener is None:
			self.initOpener(self.assembleUrl({'proto':addr['proto'], 'host':addr['host']}))
		fullAddr = self.assembleUrl(addr)
		
		#retrieve the request
		print "retrieving: " + fullAddr
		return self.opener.open(fullAddr)

	def getVideoList(self, startAddr, threadTask = None):
		req = self.openXmlPath(startAddr)
		if 'TiVoContainer' in req:
			req = req['TiVoContainer']
			if type(req['Item']) == type([]):
				thisReq = req
				while threadTask is None or threadTask.threadStatus == ThreadTask.thrRUNNING:
					if int(thisReq['ItemStart']) + int(thisReq['ItemCount']) < int(thisReq['Details']['TotalItems']):
						newAddr = dict(startAddr)
						newAddr['args']['AnchorOffset'] = int(thisReq['ItemStart']) + self.REQUEST_SIZE
						thisReq = self.openXmlPath(newAddr)
						if 'TiVoContainer' in thisReq:
							thisReq = thisReq['TiVoContainer']
							if type(thisReq['Item']) != type([]):
								thisReq['Item'] = [thisReq['Item']]
							req['Item'] = req['Item'] + thisReq['Item']
							req['ItemCount'] = int(req['ItemCount']) + int(thisReq['ItemCount'])
						else:
							break	# protocol fault?
					else:
						break
			contType = req['Details']['ContentType']
			if contType == 'x-tivo-container/tivo-server':
				return self.handleFolderList(req, threadTask)
			elif contType == 'x-tivo-container/tivo-videos':
				return self.handleVideoList(req)

	def handleFolderList(self, req, threadTask = None):
		folders = req['Item']
		if type(folders) != type([]):
			folders = [folders]
		for folder in folders:
			contType = folder['Details']['ContentType']
			if contType == 'x-tivo-container/tivo-videos':
				return self.getVideoList(self.crackUrl(folder['Links']['Content']['Url']), threadTask)

	def tivoId(self, item):
		if 'TiVoVideoDetails' in item['Links']:
			cracked = self.crackUrl(item['Links']['TiVoVideoDetails']['Url'])
			return int(cracked['args']['id'])

	def handleVideoList(self, req):
		videos = {}
		items = req['Item']
		if type(items) != type([]):
			items = [items]
		for item in req['Item']:
			if 'Available' in item['Links']['Content'] and item['Links']['Content']['Available'] == 'No':
				pass
			else:
				videos[self.tivoId(item)] = self.handleVideo(item)
		return videos

	def handleVideo(self, item):
		details = item['Details']
		if 'SourceSize' in details:
			details['SourceSize'] = int(details['SourceSize'])
		if 'Duration' in details:
			details['Duration'] = int(details['Duration'])
		if 'SourceChannel' in details:
			details['SourceChannel'] = int(details['SourceChannel'])
		if 'ByteOffset' in details:
			details['ByteOffset'] = int(details['ByteOffset'])
		if 'EpisodeNumber' in details:
			details['EpisodeNumber'] = int(details['EpisodeNumber'])
		if 'CaptureDate' in details:
			details['CaptureDate'] = int(float.fromhex(details['CaptureDate']))
		return item

###############################################################################
class TivoServer(Asset):
	def __init__(self, attr):
		Asset.__init__(self, 'TivoServer')
		self.resetAttrs(attr)

	def resetAttrs(self, attr):
		self.rawattr = attr
		self.attr = {}
		self.id = attr['identity']
		self.attr['id'] = self.id
		if 'machine' in self.rawattr:
			self.attr['name'] = self.rawattr['machine']

	def name(self):
		name = self.id
		if 'name' in self.attr:
			name = self.attr['name'] + " (" + self.id + ")"
		return name

	def tivoAddr(self):
		if ('services' in self.rawattr) and ('TiVoMediaServer' in self.rawattr['services']):
			service = self.rawattr['services']['TiVoMediaServer']
			addr = { 'proto':'http', 'host':self.rawattr['address'] }
			if 'proto' in service:
				addr['proto'] = service['proto']
			if 'port' in service:
				addr['port'] = service['port']
			return addr

	def satisfies(self, require):
		if self.type != require.type:
			return False
		if not isinstance(require, AssetPlaceholder):
			return self == require
		if 'id' in require.attr and not isinstance(require.attr['id'], base.TransformPlaceholder) and require.attr['id'] != self.id:
			return False
		return True

class TivoServerVideoDiscoveryTransform(Transform):
	SPEC = base.TransformSpec()
	SPEC.requires = [AssetPlaceholder('TivoServer', {'id': base.TransformPlaceholder(), 'mediaKey': base.TransformPlaceholder() })]
	SPEC.produces = [AssetPlaceholder('TivoVideo',  {'server': SPEC.requires[0].id })]
	
	def __init__(self):
		Transform.__init__(self)
	def spec(self):
		return self.SPEC
	def newTask(self,env,input,output):
		if input is None:
			raise TaskLaunchError('inappropriate input arguments')
		tivo = input[0]
		return TivoServerVideoDiscovery(self, tivo, env)
	def isRunning(self,tasks,input,output):
		if input is None:
			raise TaskLaunchError('inappropriate input arguments')
		tivo = input[0]
		for task in tasks:
			if task.tivo == tivo:
				return True
		return False

class TivoServerVideoDiscovery(ThreadTask):
	def __init__(self, xform, tivo, env):
		ThreadTask.__init__(self)
		self.xform = xform
		self.tivo = tivo
		self.env = env
	
	def name(self):
		return "Tivo Video Discovery %s" % self.tivo
	def stop(self):
		self.threadStatus = ThreadTask.thrCANCELLING
	def transform(self):
		return self.xform
		
	def run(self):
		addr = self.tivo.tivoAddr()
		if addr is not None:
			videoList = TivoServerQuery(self.tivo.mediaKey).getVideoList(addr, self)
			videos = env.getAssetsByType('TivoVideo')
			
			# check for updated videos
			newVideos = dict(videoList)
			if videos is not None:
				for video in videos:
					if video.server == self.tivo:
						if video.tivoId in newVideos:
							del newVideos[video.tivoId]
						else:
							env.undeclareAsset(video)
							video.close()

			# check for added assets
			for videoKey in newVideos:
				newVideo = TivoVideo(videoKey, self.tivo, videoList[videoKey])
				env.declareAsset(newVideo)

###############################################################################
class TivoVideo(Asset):
	def __init__(self, tivoId, server, attr):
		Asset.__init__(self, 'TivoVideo')
		self.details = attr['Details']
		self.links = attr['Links']
		self.tivoId = tivoId
		self.server = server
		self.attr['server'] = server.id
		self.attr['showid'] = self.showId()
		self.attr['programid'] = self.programId()
		if 'Title' in self.details:
			self.attr['title'] = self.details['Title']

	def programId(self):
		if 'programid' in self.attr:
			return self.attr['programid']
		id = self.details['ProgramId']
		show = ('00' + id[2:-4])[-8:]
		episode = id[-4:]
		if episode == '0000':
			return self.showId()
		return '%s%s%s' % (id[:2], show, episode)

	def showId(self):
		if 'showid' in self.attr:
			return self.attr['showid']
		id = self.details['ProgramId']
		show = ('00' + id[2:-4])[-8:]
		eptype = id[:2]
		if eptype == 'EP':
			eptype = 'SH'
		return '%s%s' % (eptype, show)

	def name(self):
		title = self.attr['title']
		if 'programid' in self.attr:
			title = '[%s] %s' % (self.attr['programid'], title)
		if 'EpisodeTitle' in self.details:
			title = '%s - %s' % (title, self.details['EpisodeTitle'])
		if 'name' in self.server.attr:
			title = '%s (on %s)' % (title, self.server.attr['name'])
		else:
			title = '%s (on %s)' % (title, self.server.id)
		if 'SourceSize' in self.details:
			title = '%s (%s)' % (title, formatFilesize(self.details['SourceSize']))
		return title

	def satisfies(self, require):
		if self.type != require.type:
			return False
		if not isinstance(require, AssetPlaceholder):
			return self == require
		if 'id' in require.attr and not isinstance(require.attr['server'], base.TransformPlaceholder) and require.attr['server'] != self.server.id:
			return False
		if 'showid' in require.attr and not isinstance(require.attr['showid'], base.TransformPlaceholder) and require.attr['showid'] != self.showId():
			return False
		if 'programid' in require.attr and not isinstance(require.attr['programid'], base.TransformPlaceholder) and require.attr['programid'] != self.programId():
			return False
		if 'title' in require.attr and not isinstance(require.attr['title'], base.TransformPlaceholder) and require.attr['title'].lower() != self.details['Title'].lower():
			return False
		return True

###############################################################################
class DownloadTivoVideo(Transform):
	SPEC = base.TransformSpec()
	SPEC.locks = [AssetPlaceholder('TivoServer', {'id': base.TransformPlaceholder(), 'mediaKey': base.TransformPlaceholder() })]
	SPEC.requires = [AssetPlaceholder('TivoVideo', {'server': SPEC.locks[0].id })]
	SPEC.produces = [AssetPlaceholder('TivoVideoDownload', {'mediaKey': SPEC.locks[0].mediaKey, '!isFile':1, 'fileExt':'tivo' })]
	
	def __init__(self):
		Transform.__init__(self)
	
	def spec(self):
		return self.SPEC

	def newTask(self,env,input,output):
		if input is None:
			raise TaskLaunchError('inappropriate input arguments')
		if output is None:
			raise TaskLaunchError('inappropriate output arguments')
		infile = input[0]
		outfile = output[0]
		mediaKey = infile.server.mediaKey
		#self.fetchFile(mediaKey, infile, outfile)
		tempAttr = dict(infile.details)
		if 'ContentType' in tempAttr:
			del tempAttr['ContentType']
		if 'SourceFormat' in tempAttr:
			del tempAttr['SourceFormat']
		if 'SourceSize' in tempAttr:
			del tempAttr['SourceSize']
		if 'ByteOffset' in tempAttr:
			del tempAttr['ByteOffset']
		for attr in tempAttr:
			outfile.details[attr] = tempAttr[attr]
		outfile.mediaKey = mediaKey

		query = TivoServerQuery(mediaKey)
		req = query.openSimplePath(query.crackUrl(infile.links['Content']['Url']))
		file = outfile.open('wb')
		return TivoDownloadTask(outfile, req, file, infile.server, env)

class FileCopyTask(ThreadTask):
	def __init__(self, asset, src, dest):
		ThreadTask.__init__(self)
		self.asset = asset
		self.src = src
		self.dest = dest
		self.block = 1024*1024
	def name(self):
		return "Stream Copy Task: %s" % self.asset
	def stop(self):
		self.threadStatus = ThreadTask.thrCANCELLING
	def run(self):
		try:
			while self.threadStatus == ThreadTask.thrRUNNING:
				buf = self.src.read(self.block)
				if not buf:
					break
				self.dest.write(buf)
		finally:
			self.src.close()
			self.dest.close()

class TivoDownloadTask(FileCopyTask):
	def __init__(self, asset, src, dest, server, env):
		FileCopyTask.__init__(self, asset, src, dest)
		self.server = server
		self.env = env
	def run(self):
		FileCopyTask.run(self)
		if self.status == task.Task.tsRUNNING:
			self.env.declareAsset(self.asset)
	

class FileAsset(Asset):
	def __init__(self, type, filename):
		Asset.__init__(self, type)
		self.filename = filename
	def name(self):
		return self.filename
	def open(self, mode):
		return open(self.filename, mode)

class TivoVideoDownload(FileAsset):
	def __init__(self, filename, mediaKey):
		FileAsset.__init__(self, 'TivoVideoDownload', filename)
		self.details = {}
		self.mediaKey = mediaKey
	def satisfies(self, require):
		if self.type != require.type:
			return False
		if not isinstance(require, AssetPlaceholder):
			return self == require
		return True

###############################################################################

class DecryptTivoVideoDSD(base.ToolchainTransform):
	SPEC = base.TransformSpec()
	SPEC.requires = [AssetPlaceholder('TivoVideoDownload', {'mediaKey': base.TransformPlaceholder(), '!isFile':1, 'fileExt':'tivo'})]
	SPEC.produces = [AssetPlaceholder('MpegVideo', {'!isFile':1, 'fileExt': 'mpg' })]
	TOOL = base.Tool({'name':'DirectShow Dump','cmd':'DSDCmd','path':'c:\program files\DirectShow Dump','url':'http://prish.com/etivo/tbr.htm'})
	
	def spec(self):
		return self.SPEC
		
	def newTask(self,env,input,output):
		if input is not None:
			raise TaskLaunchError('inappropriate input arguments')
		if output is not None:
			raise TaskLaunchError('inappropriate output arguments')
		infile = input[0]
		outfile = output[0]
		self.callTool(self.toolPath + ' -s "%s" -t "%s"' % (infile.filePath, outfile.filePath))
		tempAttr = dict(infile.details)
		if 'Duration' in tempAttr:
			del tempAttr['Duration']
		for attr in tempAttr:
			outfile.meta[attr] = tempAttr[attr]

class DecryptTivoVideoTD(base.ToolchainTransform):
	SPEC = base.TransformSpec()
	SPEC.requires = [AssetPlaceholder('TivoVideoDownload', {'mediaKey': base.TransformPlaceholder(), '!isFile':1, 'fileExt':'tivo'})]
	SPEC.produces = [AssetPlaceholder('MpegVideo', {'!isFile':1, 'fileExt': 'mpg' })]
	TOOL = base.Tool({'name':'tivodecode','cmd':'tivodecode','url':'http://tivodecode.sourceforge.net'})
	
	def spec(self):
		return self.SPEC
		
	def newTask(self,env,input,output):
		if input is not None:
			raise TaskLaunchError('inappropriate input arguments')
		if output is not None:
			raise TaskLaunchError('inappropriate output arguments')
		infile = input[0]
		outfile = output[0]
		self.callTool(self.toolPath + ' --mak "%s" --out "%s" "%s"' % (infile.mediaKey, outfile.filePath, infile.filePath))
		tempAttr = dict(infile.details)
		if 'Duration' in tempAttr:
			del tempAttr['Duration']
		for attr in tempAttr:
			outfile.meta[attr] = tempAttr[attr]

###############################################################################

#try:
#	avail_transforms
#except NameError:
#	avail_transforms = set()

task.avail_transforms.add(TivoServerListenerTransform())
task.avail_transforms.add(TivoServerVideoDiscoveryTransform())
task.avail_transforms.add(DownloadTivoVideo())
task.avail_transforms.add(DecryptTivoVideoDSD())
task.avail_transforms.add(DecryptTivoVideoTD())

###############################################################################
if __name__ == '__main__':
	import taskui
	env = task.ObservableEnvironment()
	
#	goalAsset = base.AssetPlaceholder('MpegVideo', {'title':'Mythbusters'})
	goalAsset = base.AssetPlaceholder('TivoVideoDownload', base.FrozenDict({'title':'Mythbusters'}))
	mythAsset = base.AssetPlaceholder('TivoVideo', {'title':'Mythbusters'})
	futuramaAsset = base.AssetPlaceholder('TivoVideo', {'title':'Futurama'})
	expanseAsset = base.AssetPlaceholder('TivoVideo', {'title':'The Expanse'})
	wormholeAsset = base.AssetPlaceholder('TivoVideo', {'title':'Through the Wormhole With Morgan Freeman'})

	goal = task.Goal(goalAsset)
	tasks = task.TaskController(env)
	goalTask = task.GoalTask(env, goal)
	tasks.addTask(goalTask)
	downloadTask = None
	rootDir = 'h:\\makestage\\'

	taskui.threadFrameTest(env, tasks)
	try:
		while True:
			tasks.handleMessages(True, 60)
			tasks.dump()
			
#			nextStep = set()
#			for element in goalTask.library:
#				if not goal.getUnresolvedSpecDependancies(element, env):
#					print "proposed: %s" % element
#					nextStep.add((element.transforms[0],element.extras))
#
##			print "begin %s steps" % nextStep
#			for next in nextStep:
#				step = next[0]
#				extras = next[1]
#				print "step: %s" % step
#				instances = goal.resolveSpecDependancies(step.spec(), env, extras)
#				runningTasks = tasks.tasksByTransform(step)
#				for instance in instances:
#					if runningTasks is None or not step.isRunning(runningTasks,instance,None):
#						print instance
##						try:
##							tasks.addTask(step.newTask(env,instance,None))
##						except TaskLaunchError as e:
##							pass
###						downloadTask = DownloadTivoVideo().newTask(env,(video,),(TivoVideoDownload('c:\\stuff\\supermake\\test.tivo', video.server.id),))
###						tasks.addTask(downloadTask)
###						break
#			print "end"

			if 'TivoVideo' in env.assetsByType and (not downloadTask or downloadTask.status != task.Task.tsRUNNING):
				videos = env.assetsByType['TivoVideo']
				shows = {}
				targets = {}
				for video in sorted(videos, key=TivoVideo.programId, reverse=True):
					#video = videos[key]
					if video.satisfies(mythAsset) or video.satisfies(futuramaAsset) or video.satisfies(expanseAsset) or video.satisfies(wormholeAsset):
						id = video.details['ProgramId']
						videoAsset = TivoVideoDownload(rootDir + id + '.tivo', video.server.id)
						if not os.path.isfile(videoAsset.filename):
							metaFile = open(rootDir + id + '.json', 'w')
							metaFile.write(json.dumps(video.details))
							metaFile.close()
							downloadTask = DownloadTivoVideo().newTask(env,(video,),(videoAsset,))
							tasks.addTask(downloadTask)
							break
#					showID = video.showId()
#					title = video.details['Title']
#					if not showID in shows:
#						shows[showID] = title
#				for showID in shows:
#					if showID[:2] == 'SH':
#						print '%s: %s' % (showID, shows[showID])
#				for showID in shows:
#					if showID[:2] != 'SH':
#						print '%s: %s' % (showID, shows[showID])
			
#			for assetType in env.assetsByType:
#				for key in env.assetsByType[assetType]:
#					print env.assetsByType[assetType][key]
			if 'TivoVideo' in env.assetsByType and (not downloadTask or (downloadTask.status != task.Task.tsQUEUED and downloadTask.status != task.Task.tsRUNNING)):
				print 'Videos seen but nothing to download!'
				for thisTask in tasks.tasks:
					if thisTask is TivoServerVideoDiscovery:
						thisTask.start()
						break;
	finally:
		print "Requesting shutdown..."
		tasks.stop()
