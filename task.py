import threading, Queue, base, copy
from base import Environment

try:
    avail_transforms
except NameError:
    avail_transforms = set()

###############################################################################

class TaskLaunchError(Exception):
	pass

class Observable(set):
	def notifyObservers(self, arg):
		for observer in self:
			observer(self, arg)

class Notification(object):
	ntSTATUS, ntPROGRESS, ntNEWASSET, ntDEADASSET, ntDELAYNOTIFY = range(5)
	TYPE_STRING = { ntSTATUS:'status', ntPROGRESS:'progress', ntNEWASSET:'new asset', ntDEADASSET:'dead asset', ntDELAYNOTIFY:'delayed notification' }
	
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
		isNew = (obj.type not in self.assetsByType) or (obj not in self.assetsByType[obj.type])
		Environment.declareAsset(self, obj)
		if isNew:
			self.observers.notifyObservers(Notification(self, Notification.ntNEWASSET, obj))

	def undeclareAsset(self, obj):
		isDead = (obj.type in self.assetsByType) and (obj in self.assetsByType[obj.type])
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
		self.tasks = set()
		self.xforms = {}
		if env is not None:
			self.observeEnvironment(env)
	def queueNotify(self, task, msg):
		self.messages.enqueue(msg)
	def addTask(self, task):
		if task in self.tasks:
			raise RuntimeError('Task is already a member of this controller');
		task.addObserver(self.queueNotify)
		self.tasks.add(task)
		xform = task.transform()
		if xform is not None:
			if xform not in self.xforms:
				self.xforms[xform] = set()
			self.xforms[xform].add(task)
		task.start(self)
	def removeTask(self, task):
		if task in self.tasks:
			task.removeObserver(self.queueNotify)
			task.stop(self)
			self.tasks.remove(task)
			xform = task.transform()
			if xform is not None:
				if xform in self.xforms and task in self.xforms[xform]:
					self.xforms[xform].remove(task)
	def tasksByTransform(self, xform):
		if xform in self.xforms:
			return self.xforms[xform]
		return None
	def addObserver(self, obj):
		self.observers.add(obj)
	def removeObserver(self, obj):
		self.observers.remove(obj)
	def observeEnvironment(self, env):
		env.addObserver(self.queueNotify)
	def unobserveEnvironment(self, env):
		env.removeObserver(self.queueNotify)
	def handleMessages(self, block=False, timeout=None):
		self.messages.handle(self.__onMessage, None, block, timeout)
	def __onMessage(self, msg):
		print 'Received message: ', msg
		self.observers.notifyObservers(msg)
	def dump(self):
		print len(self.tasks), "tasks listed"
		idx = 1
		for task in self.tasks:
			print "%d: %s (%s)" % (idx, task.name(), Task.STATUS_STRING[task.status])
			idx += 1
	def stop(self):
		for task in self.tasks:
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
		#print "isDependancyResolved: %s" % phAsset
		for foundAsset in assets:
			#print "-->looking at %s" % foundAsset
			if phAsset.type == foundAsset.type and foundAsset.satisfies(phAsset):
				return foundAsset

	def resolveDependancy(self, phAsset, env):
		assert(env is not None)
		assets = env.getAssetsByType(phAsset.type)
		if assets is None:
			return None
		results = []
		for foundAsset in assets:
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

	def __transformSearch(self, phAsset, isWeak = True):
		#global avail_transforms		not necessary as we're only reading?
		specs = set()
		for transform in avail_transforms:
			spec = transform.spec()
			if spec.transforms is None:
				spec.transforms = tuple([ transform ])
			assert(spec is not None)
			if spec.canProduce(phAsset):
				specs.add(spec)
			elif isWeak and isinstance(phAsset, base.AssetPlaceholder):
				product = spec.canWeaklyProduce(phAsset)
				if product is not None:
					extras = phAsset.weaklySatisfiedBy(product)
					newSpec = copy.copy(spec)
					if newSpec.extras is None:
						newSpec.extras = {}
					for extraKey in extras:
						newSpec.extras[extraKey] = extras[extraKey]
					specs.add(newSpec)
		return specs

	# what requirements does the spec have that are not satisfied by the env (if specified) ?
	def getUnresolvedSpecDependancies(self, spec, env = None):
		depends = set()
		if spec.requires is not None:
			for val in spec.requires:
				#newVal = self.combinePh(val, spec.extras)
				newVal = val
				if env is None or not self.isDependancyResolved(newVal, env):
					depends.add(newVal)
		if spec.consumes is not None:
			for val in spec.consumes:
				#newVal = self.combinePh(val, spec.extras)
				newVal = val
				if env is None or not self.isDependancyResolved(newVal, env):
					depends.add(newVal)
		if spec.locks is not None:
			for val in spec.locks:
				#newVal = self.combinePh(val, spec.extras)
				newVal = val
				if env is None or not self.isDependancyResolved(newVal, env):
					depends.add(newVal)
		return depends
		
	def combinePh(self, val, extras):
		if not isinstance(val, base.AssetPlaceholder) or extras is None:
			return val
		newVal = val.copy()
		newVal.attr.update(extras)
		return newVal
		
	def resolveSpecDependancies(self, spec, env, extras):
		input = [()]
		specInputs = []
		if spec.requires is not None:
			for val in spec.requires:
				newVal = self.combinePh(val, extras)
				input = self.__mixSpecDependency(input, specInputs, newVal, env)
				specInputs.append(newVal)
		if spec.consumes is not None:
			for val in spec.consumes:
				newVal = self.combinePh(val, extras)
				input = self.__mixSpecDependency(input, specInputs, newVal, env)
				specInputs.append(newVal)
		if spec.locks is not None:
			for val in spec.locks:
				newVal = self.combinePh(val, extras)
				input = self.__mixSpecDependency(input, specInputs, newVal, env)
				specInputs.append(newVal)
		#print input
		return input
		
	def __mixSpecDependency(self, input, specInputs, val, env):
		newInput = []
		for itemList in input:
			assert(len(itemList) == len(specInputs))
			thisVal = val
			if itemList:
				replList = {}
				for idx in range(len(itemList)):
					specInput = specInputs[idx]
					if isinstance(specInput, base.AssetPlaceholder):
						specInput.findReplacements(itemList[idx], replList)
				thisVal = thisVal.copy()
				thisVal.instantiatePlaceholder(replList)
			avail = self.resolveDependancy(thisVal, env)
			#print "%s -> %s" % (thisVal, avail)
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
		self.stale = False
		print "%d goals collected in library" % len(self.library)
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
			if not self.stale:
				self.stale = True
				self.tasks.queueNotify(self, Notification(self, Notification.ntDELAYNOTIFY, self))
		elif msg.type == Notification.ntDELAYNOTIFY:
			self.stale = False
			self.evaluate()
	def evaluate(self):
		#availChain = set()
		nextStep = set()
		for element in self.library:
			if not self.goal.getUnresolvedSpecDependancies(element, self.env):
				#availChain.add(element)
				nextStep.add((element.transforms[0],element.extras))

		#print "%d steps(s) identified from %d available path(s)" % (len(nextStep), len(availChain))
		for next in nextStep:
			step = next[0]
			extras = next[1]
			instances = self.goal.resolveSpecDependancies(step.spec(), self.env, extras)
			runningTasks = self.tasks.tasksByTransform(step)
			for instance in instances:
				if runningTasks is None or not step.isRunning(runningTasks,instance,None):
					try:
						self.tasks.addTask(step.newTask(self.env,instance,None))
					except TaskLaunchError as e:
						pass
