import threading, Queue
from base import Observable

try:
    avail_transforms
except NameError:
    avail_transforms = set()

###############################################################################

class Notification(object):
	ntSTATUS, ntPROGRESS = range(2)
	TYPE_STRING = { ntSTATUS:'status', ntPROGRESS:'progress' }
	
	def __init__(self, task, type, value):
		self.task = task
		self.type = type
		self.value = value
	def __repr__(self):
		return "<Notification %s: %s %s>" % (repr(self.task), Notification.TYPE_STRING[self.type], repr(self.value))
		
class MessageQueue(object):
	def __init__(self):
		self.queue = Queue.Queue()
	def enqueue(self, obj):
		self.queue.put(obj)
	def empty(self):
		return self.queue.empty()
	def dequeue(self):
		try:
			return self.queue.get_nowait()
		except Queue.Empty:
			return None
	def handle(self, dispatch, args=None, block=False, timeout=None):
		while True:
			try:
				thisArg = []
				if args is not None:
					thisArg = list(args)
				thisArg.append(self.queue.get(block, timeout))
			except Queue.Empty:
				return
			dispatch(*thisArg)

class Progress(object):
	def __init__(self, pos, max):
		self.pos = pos
		self.max = max
	
class Task(object):
	tsQUEUED, tsRUNNING, tsCOMPLETE, tsCANCELLED, tsFAILED = range(5)
	STATUS_STRING = { tsQUEUED:'queued', tsRUNNING:'running', tsCOMPLETE:'complete', tsCANCELLED:'cancelled', tsFAILED:'failed' }

	def __init__(self):
		self.status = Task.tsQUEUED
		self.progress = Progress(0,0)
		self.observers = Observable()
	def addObserver(self, obj):
		self.observers.add(obj)
	def start(self):
		pass
	def stop(self):
		pass
	def name(self):
		return "Generic Task"
	def statusChanged(self, newStatus):
		self.status = newStatus
		self.observers.notifyObservers(Notification(self, Notification.ntSTATUS, newStatus))

class ThreadTask(Task):
	thrSTOPPED, thrSTARTING, thrRUNNING, thrSTOPPING, thrCANCELLING = range(5)
	THREADSTATUS_STRING = { thrSTOPPED:'stopped', thrSTARTING:'starting', thrRUNNING:'running', thrSTOPPING:'stopping', thrCANCELLING:'cancelling' }

	def __init__(self):
		Task.__init__(self)
		self.threadStatus = ThreadTask.thrSTOPPED
		self.thread = None
	def start(self):
		if self.thread is None:
			self.thread = threading.Thread(target=self.threadStart, name=self.name())
		if self.threadStatus == ThreadTask.thrSTOPPING:
			self.thread.join()
		if not self.thread.isAlive():
			self.threadStatus = ThreadTask.thrSTARTING
			self.thread.start()
	def threadStart(self):
		self.threadStatus = ThreadTask.thrRUNNING
		self.statusChanged(Task.tsRUNNING)
		try:
			self.run()
		except (KeyboardInterrupt, SystemExit):
			pass
		except:
			if self.status == Task.tsRUNNING:
				self.statusChanged(Task.tsFAILED)
			raise
		finally:
			if self.status == Task.tsRUNNING:
				if self.threadStatus == ThreadTask.thrCANCELLING:
					self.statusChanged(Task.tsCANCELLED)
				else:
					self.statusChanged(Task.tsCOMPLETE)
			self.threadStatus = ThreadTask.thrSTOPPING
	def run(self):
		pass
		
class TaskController(object):
	def __init__(self):
		self.messages = MessageQueue()
		self.observers = Observable()
		self.tasks = set()
	def notify(self, task, msg):
		self.messages.enqueue(msg)
	def addTask(self, task):
		if task in self.tasks:
			raise RuntimeError('Task is already a member of this controller');
		task.addObserver(self.notify)
		self.tasks.add(task)
		task.start()
	def addObserver(self, obj):
		self.observers.add(obj)
	def handleMessages(self, block=False, timeout=None):
		self.messages.handle(self.onMessage, None, block, timeout)
	def onMessage(self, msg):
		print 'Received message: ', msg
		self.observers.notifyObservers(msg)
	def dump(self):
		print len(self.tasks), "tasks listed"
		idx = 1
		for task in self.tasks:
			print "%d: %s (%s)" % (idx, task.name(), Task.STATUS_STRING[task.status])
			idx += 1

###############################################################################

class Goal(object):
	def __init__(self, ph):
		assert(ph is not None)
		self.placeholder = ph
		
	# can the specified environment contain something that satisfies this asset requirement?
	def isDependancyResolved(self, phAsset, env):
		assert(env is not None)
		assets = env.getAssetsByType(phAsset.type)
		if assets != None:
			for assetKey in assets.keys():
				foundAsset = assets[assetKey]
				if phAsset.satisfiedBy(foundAsset):
					return foundAsset
		return None

	# what steps can we do to get to an asset with the specified pattern?
	def findChain(self, env = None):
		if env is not None:
			localAsset = self.isDependancyResolved(self.placeholder, env)
			if localAsset is not None:
				result = set([localAsset]);
				return result
		return self.findChainBlindly(env)
	
	# same as findChain, except don't look for already-existing assets
	def findChainBlindly(self, env = None):
		specs = set()
		chains = self.transformSearch(self.placeholder)
		while chains:
			for element in chains:
				#print "deps came back for " + str(element)
				depends = self.getUnresolvedDependancies(element, env)
				if not depends or env is None:
					specs.add(element)
			chains = self.transformExtend(chains, env)
		return specs

	# what requirements does the spec have that are not satisfied by the env (if specified) ?
	def getUnresolvedDependancies(self, spec, env = None):
		depends = set()
		if spec.requires is not None:
			for val in spec.requires:
				if env is None or not self.isDependancyResolved(val, env):
					depends.add(val)
		if spec.consumes is not None:
			for val in spec.consumes:
				if env is None or not self.isDependancyResolved(val, env):
					depends.add(val)
		if spec.locks is not None:
			for val in spec.locks:
				if env is None or not self.isDependancyResolved(val, env):
					depends.add(val)
		return depends
		
	def transformSearch(self, phAsset):
		#global avail_transforms		not necessary as we're only reading?
		specs = set()
		for transform in avail_transforms:
			spec = transform.spec()
			assert(spec is not None)
			if spec.canProduce(phAsset):
				if spec.transforms is None:
					spec.transforms = tuple([ transform ])
				specs.add(spec)
		return specs
		
	# given a list of specs, attempt to find other transforms to add onto it to extend the possible chain
	def transformExtend(self, specs, env = None):
		extendedSpecs = set()
		for spec in specs:
			#print "trying to extend " + str(spec)
			deps = self.getUnresolvedDependancies(spec, env)
			for dep in deps:
				theseSpecs = self.transformSearch(dep)
				for thisSpec in theseSpecs:
					if thisSpec.joinableAsChild(spec):
						print "COMBINE " + str(thisSpec) + " + " + str(spec)
						combinedSpec = thisSpec.combineAsChild(spec)
						print "COMBINE-result " + str(combinedSpec)
						extendedSpecs.add(combinedSpec)
		return extendedSpecs
