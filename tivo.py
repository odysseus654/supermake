import socket, traceback, time, thread, urllib2, urllib, urlparse, sys, xml.sax
import base, task
from base import Asset, Transform, AssetPlaceholder
from task import ThreadTask

###############################################################################
class TivoServerListenerTransform(Transform):
	SPEC = base.TransformSpec()
	SPEC.produces = [AssetPlaceholder('TivoServer', {'id': base.TransformPlaceholder(), 'mediaKey': base.TransformPlaceholder() })]
	
	def __init__(self):
		Transform.__init__(self)
	def spec(self):
		return self.SPEC
	def task(self,env,input,output):
		if input is not None:
			raise ArgumentError('inappropriate input arguments')
		return self.newTask(TivoServerListenerTask(env))
	
class TivoServerListenerTask(ThreadTask):
	SOCKET_PORT    = 2190
	SERVER_STALE   = 120
	SERVER_EXPIRE  = 300
	SOCKET_TIMEOUT = 60

	def __init__(self, env):
		ThreadTask.__init__(self)
		self.tivoStatus = {}					# no lock necessary as it's only ever looked at by the worker thread
		self.env = env
	
	def name(self):
		return "Tivo Broadcast Listener"
	def stop(self):
		self.threadStatus = ThreadTask.thrCANCELLING
		
	def serverSeen(self, id, attrs):
		if (id in self.tivoStatus) and (self.tivoStatus[id]['attr'] == attrs):
			self.tivoStatus[id]['lastSeen'] = time.time()
			if 'stale' in self.tivoStatus[id]:
				del self.tivoStatus[id]['stale']
				self.tivoStatus[id]['asset'].delFlag(Asset.flagSTALE)
		elif id in self.tivoStatus:				# this only happens if something serious changed, like a version upgrade or the tivo rebooted or something
			self.tivoStatus[id]['lastSeen'] = time.time()
			if 'stale' in self.tivoStatus[id]:
				del self.tivoStatus[id]['stale']
				self.tivoStatus[id]['asset'].delFlag(Asset.flagSTALE)
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
		auth.add_password('TiVo DVR', addr, 'tivo', self.mediaKey)
		self.opener = urllib2.build_opener(auth)

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
		if not('path' in addr):
			addr['path'] = '/TiVoConnect'
		if not('args' in addr):
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

	def getVideoList(self, startAddr):
		req = self.openXmlPath(startAddr)
		if 'TiVoContainer' in req:
			if type(req['TiVoContainer']['Item']) == type([]):
				thisReq = req
				while 1:
					if int(thisReq['TiVoContainer']['ItemStart']) + int(thisReq['TiVoContainer']['ItemCount']) < int(thisReq['TiVoContainer']['Details']['TotalItems']):
						newAddr = dict(startAddr)
						newAddr['args']['AnchorOffset'] = int(thisReq['TiVoContainer']['ItemStart']) + self.REQUEST_SIZE
						thisReq = self.openXmlPath(newAddr)
						if 'TiVoContainer' in thisReq:
							if type(thisReq['TiVoContainer']['Item']) == type([]):
								req['TiVoContainer']['Item'] = req['TiVoContainer']['Item'] + thisReq['TiVoContainer']['Item']
							else:
								req['TiVoContainer']['Item'] = req['TiVoContainer']['Item'] + [thisReq['TiVoContainer']['Item']]
							req['TiVoContainer']['ItemCount'] = int(req['TiVoContainer']['ItemCount']) + int(thisReq['TiVoContainer']['ItemCount'])
					else:
						break
			contType = req['TiVoContainer']['Details']['ContentType']
			if contType == 'x-tivo-container/tivo-server':
				return self.handleFolderList(req)
			elif contType == 'x-tivo-container/tivo-videos':
				return self.handleVideoList(req)

	def handleFolderList(self, req):
		if type(req['TiVoContainer']['Item']) == type([]):
			for folder in req['TiVoContainer']['Item']:
				contType = folder['Details']['ContentType']
				if contType == 'x-tivo-container/tivo-videos':
					return self.getVideoList(self.crackUrl(folder['Links']['Content']['Url']))
		else:
			folder = req['TiVoContainer']['Item']
			contType = folder['Details']['ContentType']
			if contType == 'x-tivo-container/tivo-videos':
				return self.getVideoList(self.crackUrl(folder['Links']['Content']['Url']))

	def tivoId(self, item):
		if 'TiVoVideoDetails' in item['Links']:
			cracked = self.crackUrl(item['Links']['TiVoVideoDetails']['Url'])
			return int(cracked['args']['id'])

	def handleVideoList(self, req):
		videos = {}
		if type(req['TiVoContainer']['Item']) == type([]):
			for item in req['TiVoContainer']['Item']:
				if 'Available' in item['Links']['Content'] and item['Links']['Content']['Available'] == 'No':
					pass
				else:
					videos[self.tivoId(item)] = self.handleVideo(item)
		else:
			item = req['TiVoContainer']['Item']
			if 'Available' in item['Links']['Content'] and item['Links']['Content']['Available'] == 'No':
				pass
			else:
				videos[self.tivoId(item)] = self.handleVideo(item)
		return videos

	def handleVideo(self, item):
		if 'SourceSize' in item['Details']:
			item['Details']['SourceSize'] = int(item['Details']['SourceSize'])
		if 'Duration' in item['Details']:
			item['Details']['Duration'] = int(item['Details']['Duration'])
		if 'SourceChannel' in item['Details']:
			item['Details']['SourceChannel'] = int(item['Details']['SourceChannel'])
		if 'ByteOffset' in item['Details']:
			item['Details']['ByteOffset'] = int(item['Details']['ByteOffset'])
		if 'EpisodeNumber' in item['Details']:
			item['Details']['EpisodeNumber'] = int(item['Details']['EpisodeNumber'])
		if 'CaptureDate' in item['Details']:
			item['Details']['CaptureDate'] = int(float.fromhex(item['Details']['CaptureDate']))
		return item

###############################################################################
class TivoServer(Asset):
	def __init__(self, attr):
		Asset.__init__(self, 'TivoServer')
		self.resetAttrs(attr)

	def resetAttrs(self, attr):
		self.attr = attr
		self.id = attr['identity']
		self.mediaKey = "4714995599"

	def ident(self):
		return self.id

	def name(self):
		machine = self.id
		if 'machine' in self.attr:
			machine = self.attr['machine']
		return machine

	def tivoAddr(self):
		if ('services' in self.attr) and ('TiVoMediaServer' in self.attr['services']):
			service = self.attr['services']['TiVoMediaServer']
			addr = { 'proto':'http', 'host':self.attr['address'] }
			if 'proto' in service:
				addr['proto'] = service['proto']
			if 'port' in service:
				addr['port'] = service['port']
			return addr

class TivoServerVideoDiscoveryTransform(Transform):
	SPEC = base.TransformSpec()
	SPEC.requires = [AssetPlaceholder('TivoServer', {'id': base.TransformPlaceholder(), 'mediaKey': base.TransformPlaceholder() })]
	SPEC.produces = [AssetPlaceholder('TivoVideo',  {'server': SPEC.requires[0].id })]
	
	def __init__(self):
		Transform.__init__(self)
	def spec(self):
		return self.SPEC
	def task(self,env,input,output):
		if input is None:
			raise ArgumentError('inappropriate input arguments')
		tivo = input[0]
		return self.newTask(TivoServerVideoDiscovery(tivo, env))

class TivoServerVideoDiscovery(ThreadTask):
	def __init__(self, tivo, env):
		ThreadTask.__init__(self)
		self.tivo = tivo
		self.env = env
	
	def name(self):
		return "Tivo Video Discovery"
	def stop(self):
		self.threadStatus = ThreadTask.thrCANCELLING
		
	def run(self):
		addr = self.tivo.tivoAddr()
		if addr is not None:
			videoList = TivoServerQuery(self.tivo.mediaKey).getVideoList(addr)
			assets = env.getAssetsByType('TivoVideo')
			
			# check for updated assets
			if assets is not None:
				for assetKey in assets.keys():
					asset = assets[assetKey]
					splitKey = asset.ident().split(':', 2)
					if splitKey[0] == self.tivo.id:
						if not (int(float.fromHex('0x' + splitKey[1])) in videoList):
							env.undeclareAsset(asset)
							asset.close()
			# check for added assets
			for videoKey in videoList:
				id = "%s:%s" % (self.tivo.id, hex(videoKey)[2:])
				if assets is None or not(id in assets):
					newVideo = TivoVideo(id, self.tivo.id, videoList[videoKey])
					env.declareAsset(newVideo)

###############################################################################
class TivoVideo(Asset):
	def __init__(self, id, server, attr):
		Asset.__init__(self, 'TivoVideo')
		self.details = attr['Details']
		self.links = attr['Links']
		self.id = id
		self.server = server

	def ident(self):
		return self.id

	def dispFilesize(self, size):
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

	def name(self):
		title = self.details['Title']
		if 'EpisodeNumber' in self.details:
			title = title + ' [' + str(self.details['EpisodeNumber']) + ']'
		if 'EpisodeTitle' in self.details:
			title = title + ' - ' + self.details['EpisodeTitle']
		if 'SourceSize' in self.details:
			title = title + ' (' + self.dispFilesize(self.details['SourceSize']) + ')'
		return title

###############################################################################
class DownloadTivoVideo(Transform):
	SPEC = base.TransformSpec()
	SPEC.locks = [AssetPlaceholder('TivoServer', {'id': base.TransformPlaceholder(), 'mediaKey': base.TransformPlaceholder() })]
	SPEC.requires = [AssetPlaceholder('TivoVideo', {'server': SPEC.locks[0].id, 'details': base.TransformPlaceholder() })]
	SPEC.produces = [AssetPlaceholder('TivoVideoDownload', {'mediaKey': SPEC.locks[0].mediaKey, 'details': SPEC.requires[0].details, '!isFile':1, 'fileExt':'tivo' })]
	
	def __init__(self):
		Transform.__init__(self)
	
	def spec(self):
		return self.SPEC

	def fetchFile(self, mediaKey, infile, outfile):
		query = TivoServerQuery(mediaKey)
		req = query.openSimplePath(query.crackUrl(infile.links['Content']['Url']))

	def task(self,input,output):
		server = input[0]
		infile = input[1]
		outfile = output[0]
		mediaKey = server.mediaKey
		self.fetchFile(mediaKey, infile, outfile)
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

###############################################################################

class DecryptTivoVideoDSD(base.ToolchainTransform):
	SPEC = base.TransformSpec()
	SPEC.requires = [AssetPlaceholder('TivoVideoDownload', {'details': base.TransformPlaceholder()})]
	SPEC.produces = [AssetPlaceholder('MpegVideo', {'!isFile':1, 'fileExt': 'mpg', 'meta': SPEC.requires[0].details })]
	TOOL = base.Tool({'name':'DirectShow Dump','cmd':'DSDCmd','path':'c:\program files\DirectShow Dump','url':'http://prish.com/etivo/tbr.htm'})
	
	def spec(self):
		return self.SPEC
		
	def task(self,input,output):
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
	SPEC.requires = [AssetPlaceholder('TivoVideoDownload', {'mediaKey': base.TransformPlaceholder(), 'details': base.TransformPlaceholder()})]
	SPEC.produces = [AssetPlaceholder('MpegVideo', {'!isFile':1, 'fileExt': 'mpg', 'meta': SPEC.requires[0].details })]
	TOOL = base.Tool({'name':'tivodecode','cmd':'tivodecode','url':'http://tivodecode.sourceforge.net'})
	
	def spec(self):
		return self.SPEC
		
	def task(self,input,output):
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
	env = base.Environment()
	goal = task.Goal(base.AssetPlaceholder('MpegVideo'))
	tasks = task.TaskController()
#	tasks.addTask(TivoServerListenerTask(env))
#	while True:
#		tasks.handleMessages(True, 30)
#		tasks.dump()
#		for assetType in env.assetsByType:
#			for key in env.assetsByType[assetType]:
#				print env.assetsByType[assetType][key]
##				if assetType == 'TivoVideo':
##					print env.assetsByType[assetType][key].details
	chain = goal.findChain()

	availChain = []
	for element in chain:
		if not goal.getUnresolvedDependancies(element, env):
			availChain.append(element)

	print str(len(availChain)) + " path(s) found"
	for element in availChain:
		print element
