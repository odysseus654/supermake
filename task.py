import threading, Queue
from base import Environment

try:
    avail_transforms
except NameError:
    avail_transforms = set()

###############################################################################

class Observable(set):
	def notifyObservers(self, arg):
		for observer in self:
			observer(self, arg)

class Notification(object):
	ntSTATUS, ntPROGRESS, ntNEWASSET, ntDEADASSET = range(4)
	TYPE_STRING = { ntSTATUS:'status', ntPROGRESS:'progress', ntNEWASSET:'new asset', ntDEADASSET:'dead asset' }
	
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
	
class ObservableEnvironment(Environment):
	def __init__(self):
		Environment.__init__(self)
		self.observers = Observable()
	def addObserver(self, obj):
		self.observers.add(obj)
	def removeObserver(self, obj):
		self.observers.remove(obj)
		
	def declareAsset(self, obj):
		ident = obj.ident()
		isNew = not(obj.type in self.assetsByType) or not (ident in self.assetsByType[obj.type])
		Environment.declareAsset(self, obj)
		if isNew:
			self.observers.notifyObservers(Notification(self, Notification.ntNEWASSET, obj))

	def undeclareAsset(self, obj):
		ident = obj.ident()
		isDead = (obj.type in self.assetsByType) and (ident in self.assetsByType[obj.type])
		Environment.undeclareAsset(self, obj)
		if isDead:
			self.observers.notifyObservers(Notification(self, Notification.ntDEADASSET, obj))

class Task(object):
	tsQUEUED, tsRUNNING, tsCOMPLETE, tsCANCELLED, tsFAILED = range(5)
	STATUS_STRING = { tsQUEUED:'queued', tsRUNNING:'running', tsCOMPLETE:'complete', tsCANCELLED:'cancelled', tsFAILED:'failed' }

	def __init__(self):
		self.status = Task.tsQUEUED
		self.progress = Progress(0,0)
		self.observers = Observable()
	def addObserver(self, obj):
		self.observers.add(obj)
	def removeObserver(self, obj):
		self.observers.remove(obj)
	def start(self, tasks = None):
		pass
	def stop(self):
		pass
	def name(self):
		return "Generic Task"
	def transform(self):
		return None
	def __repr__(self):
		return "<Task: %s>" % self.name()
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
	def start(self, tasks = None):
		if self.thread is None:
			self.thread = threading.Thread(target=self.__threadStart, name=self.name())
		if self.threadStatus == ThreadTask.thrSTOPPING:
			self.thread.join()
		if not self.thread.isAlive():
			self.threadStatus = ThreadTask.thrSTARTING
			self.thread.start()
	def __threadStart(self):
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
	def __repr__(self):
		return "<ThreadTask: %s>" % self.name()
		
class TaskController(object):
	def __init__(self, env = None):
		self.messages = MessageQueue()
		self.observers = Observable()
		self.tasks = {}
		self.xforms = {}
		if env is not None:
			self.observeEnvironment(env)
	def __notify(self, task, msg):
		self.messages.enqueue(msg)
	def addTask(self, task):
		taskId = id(task)
		if taskId in self.tasks:
			raise RuntimeError('Task is already a member of this controller');
		task.addObserver(self.__notify)
		self.tasks[taskId] = task
		xform = task.transform()
		if xform is not None:
			xformID = id(xform)
			if not(xformID in self.xforms):
				self.xforms[xformID] = {}
			self.xforms[xformID][taskId] = task
		task.start(self)
	def removeTask(self, task):
		taskId = id(task)
		if taskId in self.tasks:
			task.removeObserver(self.__notify)
			task.stop(self)
			del self.tasks[taskId]
			xform = task.transform()
			if xform is not None:
				xformID = id(xform)
				if xformID in self.xforms and taskId in self.xforms[xformID]:
					del self.xforms[xformID][taskId]
	def tasksByTransform(self, xform):
		xformID = id(xform)
		if xformID in self.xforms:
			return self.xforms[xformID]
		return None
	def addObserver(self, obj):
		self.observers.add(obj)
	def removeObserver(self, obj):
		self.observers.remove(obj)
	def observeEnvironment(self, env):
		env.addObserver(self.__notify)
	def unobserveEnvironment(self, env):
		env.removeObserver(self.__notify)
	def handleMessages(self, block=False, timeout=None):
		self.messages.handle(self.__onMessage, None, block, timeout)
	def __onMessage(self, msg):
		print 'Received message: ', msg
		self.observers.notifyObservers(msg)
	def dump(self):
		print len(self.tasks), "tasks listed"
		idx = 1
		for taskId in self.tasks:
			task = self.tasks[taskId]
			print "%d: %s (%s)" % (idx, task.name(), Task.STATUS_STRING[task.status])
			idx += 1
	def stop(self):
		for taskId in self.tasks:
			task = self.tasks[taskId]
			task.stop()

###############################################################################

class Goal(object):
	def __init__(self, ph):
		self.placeholder = ph
		
	# can the specified environment contain something that satisfies this asset requirement?
	def isDependancyResolved(self, phAsset, env):
		assert(env is not None)
		assets = env.getAssetsByType(phAsset.type)
		if assets is None:
			return None
		#print "isDependencyResolved: %s" % phAsset
		for assetKey in assets.keys():
			foundAsset = assets[assetKey]
			#print "-->looking at %s" % foundAsset
			if phAsset.type == foundAsset.type and foundAsset.satisfies(phAsset):
				return foundAsset

	def resolveDependancy(self, phAsset, env):
		assert(env is not None)
		assets = env.getAssetsByType(phAsset.type)
		if assets is None:
			return None
		results = []
		for assetKey in assets.keys():
			foundAsset = assets[assetKey]
			if phAsset.type == foundAsset.type and foundAsset.satisfies(phAsset):
				results.append(foundAsset)
		return results

	# what steps can we do to get to an asset with the specified pattern?
	def findChain(self, env = None):
	#	if env is not None:
	#		localAsset = self.isDependancyResolved(self.placeholder, env)
	#		if localAsset is not None:
	#			return set([localAsset])

		specs = set()
		chains = self.__transformSearch(self.placeholder)
		while chains:
			for element in chains:
				#print "deps came back for " + str(element)
				if env is not None:
					depends = self.getUnresolvedSpecDependancies(element, env)
					if not depends:
						specs.add(element)
				else:
					specs.add(element)
			# given a list of specs, attempt to find other transforms to add onto it to extend the possible chain
			extendedSpecs = set()
			for spec in chains:
				#print "trying to extend " + str(spec)
				deps = self.getUnresolvedSpecDependancies(spec, env)
				for dep in deps:
					theseSpecs = self.__transformSearch(dep)
					for thisSpec in theseSpecs:
						if thisSpec.joinableAsChild(spec):
							#print "COMBINE " + str(thisSpec) + " + " + str(spec)
							combinedSpec = thisSpec.combineAsChild(spec)
							#print "COMBINE-result " + str(combinedSpec)
							extendedSpecs.add(combinedSpec)
			chains = extendedSpecs
		return specs

	def __transformSearch(self, phAsset):
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

	# what requirements does the spec have that are not satisfied by the env (if specified) ?
	def getUnresolvedSpecDependancies(self, spec, env = None):
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
		
	def resolveSpecDependancies(self, spec, env):
		input = [()]
		if spec.requires is not None:
			for val in spec.requires:
				input = self.__mixSpecDependency(input, val, env)
		if spec.consumes is not None:
			for val in spec.consumes:
				input = self.__mixSpecDependency(input, val, env)
		if spec.locks is not None:
			for val in spec.locks:
				input = self.__mixSpecDependency(input, val, env)
		return input
		
	def __mixSpecDependency(self, input, val, env):
		newInput = []
		avail = self.resolveDependancy(val, env)
		for itemList in input:
			for availItem in avail:
				newItem = list(itemList)
				newItem.append(availItem)
				newInput.append(tuple(newItem))
		return newInput

	def __repr__(self):
		return "<Goal: %s>" % repr(self.placeholder)

class GoalTask(Task):
	def __init__(self, env, goal, findAll = True):
		Task.__init__(self)
		self.env = env
		self.goal = goal
		self.findAll = findAll
		self.library = self.goal.findChain()
	def start(self, tasks):
		self.statusChanged(Task.tsRUNNING)
		self.tasks = tasks
		tasks.addObserver(self.__notify)
		self.evaluate()
	def stop(self):
		self.statusChanged(Task.tsQUEUED)
		if self.tasks is not None:
			self.tasks.removeObserver(self.__notify)
	def name(self):
		return "Goal: " + repr(self.goal)
	def __notify(self, task, msg):
		if (msg.type == Notification.ntNEWASSET) or (msg.type == Notification.ntDEADASSET):
			self.evaluate()
	def evaluate(self):
		availChain = set()
		nextStep = set()
		for element in self.library:
			if not self.goal.getUnresolvedSpecDependancies(element, self.env):
				availChain.add(element)
				nextStep.add(element.transforms[0])

		print "%d steps(s) identified from %d available path(s)" % (len(nextStep), len(availChain))
		for step in nextStep:
			instances = self.goal.resolveSpecDependancies(step.spec(), self.env)
			runningTasks = self.tasks.tasksByTransform(step)
			for instance in instances:
				if runningTasks is None or not step.isRunning(runningTasks,instance,None):
					self.tasks.addTask(step.newTask(self.env,instance,None))
