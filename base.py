import copy

class Asset(object):
	def __init__(self, type):
		self.type = type
		self.attr = {}
	def ident(self):
		pass
	def name(self):
		pass
	def close(self):
		pass
	def __repr__(self):
		ourName = self.name()
		if ourName is not None:
			return '<' + self.type + ': ' + self.name().encode('ascii', 'replace') + '>'
		else:
			return '<' + self.type + '>'

class Transform(object):
	def __init__(self):
		pass
	def spec(self):
		pass
	def task(self,env,input,output):
		pass
	def newTask(self, task):
		return task
		
class TransformSpec(object):
	def __init__(self, attr = {}):
		self.requires = attr.get('requires', None)
		if self.requires is not None:
			self.requires = frozenset(self.requires)
		self.consumes = attr.get('consumes', None)
		if self.consumes is not None:
			self.consumes = frozenset(self.consumes)
		self.locks = attr.get('locks', None)
		if self.locks is not None:
			self.locks = frozenset(self.locks)
		self.produces = attr.get('produces', None)
		if self.produces is not None:
			self.produces = frozenset(self.produces)
		self.transforms = None
	def __repr__(self):
		result = '<Transform:'
		if self.requires is not None:
			result = result + ' requires=' + str(self.requires)
		if self.consumes is not None:
			result = result + ' consumes=' + str(self.consumes)
		if self.locks is not None:
			result = result + ' locks=' + str(self.locks)
		if self.produces is not None:
			result = result + ' produces=' + str(self.produces)
		if self.transforms is not None:
			result = result +  'transforms=' + str(self.transforms)
		return result + '>'

	def __ne__(self, other):
		return self.transforms != other.transforms
	def __eq__(self, other):
		return self.transforms == other.transforms
	def __hash__(self):
		return hash(self.transforms)

	def copy(self):
		copy = TransformSpec()
		copy.requires = self.requires
		copy.consumes = self.consumes
		copy.locks = self.locks
		copy.produces = self.produces
		copy.transforms = self.transforms
		return copy
	def canProduce(self, asset):
		if self.produces is None:
			return None
		for val in self.produces:
			if asset.satisfiedBy(val):
				return val
		return None
	def joinableAsChild(self, child):
		if self.produces is None:
			return False

		# with the "throw everything against the wall" findChain() does we can come
		# up with some really weird transforms.  This is an attempt to block any out-of-order
		# transforms by preventing two transformations to be combined if they each produce
		# something the other requires
		if self.requires is not None:
			for val in self.requires:
				if child.canProduce(val) is not None:
					return False

		if child.requires is not None:
			for val in child.requires:
				if self.canProduce(val) is not None:
					return True
		if child.consumes is not None:
			for val in child.consumes:
				if self.canProduce(val) is not None:
					return True
		if child.locks is not None:
			for val in child.locks:
				if self.canProduce(val) is not None:
					return True
		return False

	def combineAsChild(self, child):
		# this does a number on both us and the child, so make copies of both first
		tempParent = self.copy()
		tempChild = child.copy()
		newSpec = TransformSpec()

		# the link is what the parent produces, so link the two together
		replList = {}
		if tempChild.requires is not None:
			tempChild.requires = list(tempChild.requires)
			for val in tempChild.requires:
				val.instantiatePlaceholder(replList)
				produces = tempParent.canProduce(val)
				if produces is not None:
					val.mergeAsset(produces, replList)
		if tempChild.consumes is not None:
			tempChild.consumes = list(tempChild.consumes)
			for val in tempChild.consumes:
				val.instantiatePlaceholder(replList)
				produces = tempParent.canProduce(val)
				if produces is not None:
					val.mergeAsset(produces, replList)
		if tempChild.locks is not None:
			tempChild.locks = list(tempChild.locks)
			for val in tempChild.locks:
				val.instantiatePlaceholder(replList)
				produces = tempParent.canProduce(val)
				if produces is not None:
					val.mergeAsset(produces, replList)
		
		# the following rules are the results of me drawing everything up on a grid, hopefully they make sense
		#		consumes		+		consumes		->		consumes(+copy)
		#		locks			+		consumes		->		consumes
		#		requires		+		consumes		->		consumes
		#		produces		+		consumes		->		(nothing)

		#		consumes		+		locks			->		locks(+copy)
		#		requires		+		locks			->		locks
		#		locks			+		locks			->		locks
		#		produces		+		locks			->		produces

		#		consumes		+		requires		->		requires(+copy)
		#		requires		+		requires		-> 		requires
		#		locks			+		requires		->		locks
		#		produces		+		requires		->		produces

		#		consumes		+		produces		->		consumes + produces
		#		requires		+		produces		->		requires + produces
		#		locks			+		produces		->		locks + produces
		#		produces		+		produces		->		produces (+del?)

		# first handle parent consumption and child production
		if tempParent.consumes is not None:
			for val in tempParent.consumes:
				if newSpec.consumes is None:
					newSpec.consumes = []
				newSpec.consumes.append(val)
		if tempChild.produces is not None:
			tempChild.produces = list(tempChild.produces)
			for val in tempChild.produces:
				val.instantiatePlaceholder(replList)
				if newSpec.produces is None:
					newSpec.produces = []
				newSpec.produces.append(val)
				
		# now handle parent production and child consumption
		parentProd = []
		if tempParent.produces is not None:
			parentProd = list(tempParent.produces)
		if tempChild.consumes is not None:
			for val in tempChild.consumes:
				val.instantiatePlaceholder(replList)
				newProd = []
				foundMatch = False
				for pVal in parentProd:
					if val == pVal and not foundMatch:
						foundMatch = True
					else:
						newProd.append(pVal)
				parentProd = newProd
				if not foundMatch:
					if newSpec.consumes is None:
						newSpec.consumes = []
					newSpec.consumes.append(val)
		for val in parentProd:
			if newSpec.produces is None:
				newSpec.produces = []
			newSpec.produces.append(val)
			
		# the rest is locks and requires
		assetLocking = {}
		if tempParent.locks is not None:
			for val in tempParent.locks:
				if val not in assetLocking:
					assetLocking[val] = True
		if tempChild.locks is not None:
			for val in tempChild.locks:
				val.instantiatePlaceholder(replList)
				foundMatch = False
				for pVal in parentProd:
					if val == pVal:
						foundMatch = True
						break
				if not foundMatch and val not in assetLocking:
					assetLocking[val] = True
		if tempParent.requires is not None:
			for val in tempParent.requires:
				if val not in assetLocking:
					assetLocking[val] = False
		if tempChild.requires is not None:
			for val in tempChild.requires:
				val.instantiatePlaceholder(replList)
				foundMatch = False
				for pVal in parentProd:
					if val == pVal:
						foundMatch = True
						break
				if not foundMatch and val not in assetLocking:
					assetLocking[val] = False
		for val in assetLocking:
			if assetLocking[val]:
				if newSpec.locks is None:
					newSpec.locks = []
				newSpec.locks.append(val)
			else:
				if newSpec.requires is None:
					newSpec.requires = []
				newSpec.requires.append(val)

		# now build a list of all the transforms that are contained in this
		newSpec.transforms = []
		if self.transforms is not None:
			newSpec.transforms.extend(self.transforms)
		else:
			newSpec.transforms.append(self)
		if child.transforms is not None:
			newSpec.transforms.extend(child.transforms)
		else:
			newSpec.transforms.append(child)

		if newSpec.requires is not None:
			newSpec.requires = frozenset(newSpec.requires)
		if newSpec.consumes is not None:
			newSpec.consumes = frozenset(newSpec.consumes)
		if newSpec.locks is not None:
			newSpec.locks = frozenset(newSpec.locks)
		if newSpec.produces is not None:
			newSpec.produces = frozenset(newSpec.produces)
		if newSpec.transforms is not None:
			newSpec.transforms = tuple(newSpec.transforms)

		return newSpec
		
class AssetPlaceholder(Asset):
	def __init__(self, type, attr = None):
		Asset.__init__(self, type)
		if attr is not None:
			self.attr = attr
	def __repr__(self):
		return '<' + self.type + ': ' + str(self.attr) + '>'
	def __getattr__(self, name):
		return self.attr[name]
	def __ne__(self, other):
		return not (self == other)
	def __eq__(self, other):
		if self.type != other.type:
			return False
		if len(self.attr) != len(other.attr):
			return False
		for attr in self.attr:
			if (attr not in other.attr) or (self.attr[attr] != other.attr[attr]):
				return False
		return True
	def __hash__(self):
		hashResult = hash(self.type) ^ hash(len(self.attr))
		for attr in self.attr:
			hashResult = hashResult ^ hash(attr)
		return hashResult
	def satisfiedBy(self, target):
		if target.type != self.type:
			return False
		for attr in self.attr:
			if attr not in target.attr:
				return False
			if not isinstance(self.attr[attr], TransformPlaceholder) and not isinstance(target.attr[attr], TransformPlaceholder) and self.attr[attr] != target.attr[attr]:
				return False
		return True
	def instantiatePlaceholder(self, replList):
		for attr in self.attr:
			if self.attr[attr] in replList:
				self.attr[attr] = replList[self.attr[attr]]
	def mergeAsset(self, other, replList = None):
		for attr in self.attr:
			if isinstance(self.attr[attr], TransformPlaceholder) and attr in other.attr:
				if replList is not None:
					replList[self.attr[attr]] = other.attr[attr]
				self.attr[attr] = other.attr[attr]
		for attr in other.attr:
			if attr not in self.attr:
				self.attr[attr] = other.attr[attr]

class TransformPlaceholder(object):
	def __repr__(self):
		return '<' + hex(id(self))[2:] + '>'

class ToolchainTransform(Transform):
	def __init__(self):
		self.toolPath = None
	def callTool(self, cmd):
		pass
	
class Tool(Asset):
	def __init__(self, attr):
		Asset.__init__(self, 'Tool')
		self.attr = attr
	def ident(self):
		return self.attr['name']
	def name(self):
		return self.attr['name']
	def __getattr__(self, name):
		return self.attr[name]
	
class Environment(object):
	def __init__(self):
		self.assetsByType = {}
		
	def declareAsset(self, obj):
		if not(obj.type in self.assetsByType):
			self.assetsByType[obj.type] = {}
		ident = obj.ident()
		if not (ident in self.assetsByType[obj.type]):
			self.assetsByType[obj.type][ident] = obj

	def undeclareAsset(self, obj):
		if obj.type in self.assetsByType:
			ident = obj.ident()
			if ident in self.assetsByType[obj.type]:
				del self.assetsByType[obj.type][ident]

	def getAssetsByType(self, type):
		if type in self.assetsByType:
	 		return self.assetsByType[type]