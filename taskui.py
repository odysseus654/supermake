import wx, wx.gizmos, threading, sys
import task

class ThreadFrameLaunch(object):
	def __init__(self, env, tasks):
		self.env = env
		self.tasks = tasks
	def start(self):
		thread = threading.Thread(target=self.__threadStart, name="ui frame test")
		thread.start()
	def __threadStart(self):
		frameTest(self.env, self.tasks)

def threadFrameTest(env, tasks):
	thread = ThreadFrameLaunch(env, tasks)
	thread.start()

EVT_NOTIFY_ID = wx.NewEventType()
class NotifyEvent(wx.PyEvent):
	def __init__(self, msg):
		wx.PyEvent.__init__(self)
		self.SetEventType(EVT_NOTIFY_ID)
		self.msg = msg
EVT_NOTIFY = wx.PyEventBinder(EVT_NOTIFY_ID)

class AssetsTab(wx.gizmos.TreeListCtrl):
	def __init__(self, parent, env):
		wx.gizmos.TreeListCtrl.__init__(self, parent=parent, id=wx.ID_ANY, style=wx.TR_DEFAULT_STYLE|wx.TR_TWIST_BUTTONS|wx.TR_HIDE_ROOT)
		self.env = env
		self.assets = dict()
		self.tivo = dict()
		self.AddColumn("")
		self.root = self.AddRoot(text="Hidden Root")
		self.SetMainColumn(0) # the one with the tree in it...
		self.Bind(EVT_NOTIFY, self.onNotifyUi)
		env.addObserver(self.onNotifyAsync)
		for type in env.assetsByType:
			for asset in env.assetsByType[type]:
				self.newAsset(asset)
	def onNotifyAsync(self, queue, msg):
		"""Received a notification on an unknown thread"""
		wx.PostEvent(self, NotifyEvent(msg))
	def onNotifyUi(self, evt):
		"""Received a notification on a UI thread"""
		msg = evt.msg
		if msg.type == task.Notification.ntNEWASSET:
			self.newAsset(msg.value)
		elif msg.type == task.Notification.ntDEADASSET:
			self.deadAsset(msg.value)
	def assetRoot(self, obj):
		root = self.root
		if obj.type == "TivoVideo":
			root = self.assets[obj.server]
			showId = obj.showId()
			if showId != obj.programId():
				shows = None
				if obj.server not in self.tivo:
					shows = dict()
					self.tivo[obj.server] = shows
				else:
					shows = self.tivo[obj.server]
				if showId not in shows:
					root = self.AppendItem(root, text=('[%s] %s' % (showId, obj.attr['title'])))
					shows[showId] = root
				else:
					root = shows[showId]
		return root
	def newAsset(self, obj):
		"""A new asset has been declared.  Assumes UI thread"""
		if obj not in self.assets:
			self.assets[obj] = self.AppendItem(self.assetRoot(obj), text=repr(obj))
	def deadAsset(self, obj):
		"""An asset has been removed.  Assumes UI thread"""
		if obj in self.assets:
			self.Delete(self.assets[obj])
			del self.assets[obj]

class TasksTab(wx.ListCtrl):
	def __init__(self, parent, tasks):
		wx.ListCtrl.__init__(self, parent=parent, id=wx.ID_ANY, style=wx.LC_REPORT)
		self.controller = tasks
		self.tasks = dict()
		self.InsertColumn(0, "")
		self.Bind(EVT_NOTIFY, self.onNotifyUi)
		self.controller.addObserver(self.onNotifyAsync)
		for thisTask in self.controller.tasks:
			if not isinstance(thisTask, task.GoalTask):
				self.newTask(thisTask, thisTask.status)
	def onNotifyAsync(self, queue, msg):
		"""Received a notification on an unknown thread"""
		wx.PostEvent(self, NotifyEvent(msg))
	def onNotifyUi(self, evt):
		"""Received a notification on a UI thread"""
		msg = evt.msg
		if not isinstance(msg.task, task.GoalTask):
			if msg.type == task.Notification.ntSTATUS:
				self.newTask(msg.task, msg.value)
		#	elif msg.type == task.Notification.ntDEADASSET:
		#		self.deadTask(msg.value)
	def newTask(self, obj, status):
		"""A new task has been declared.  Assumes UI thread"""
		id = hash(obj)
		if id not in self.tasks:
			pos = self.InsertStringItem(sys.maxint, repr(obj))
			self.tasks[id] = pos
			self.SetItemData(pos, id)
	#def deadTask(self, obj):
	#	"""An asset has been removed.  Assumes UI thread"""
	#	if obj in self.tasks:
	#		self.Delete(self.tasks[obj])
	#		del self.tasks[obj]

class GoalsTab(wx.ListCtrl):
	def __init__(self, parent, tasks):
		wx.ListCtrl.__init__(self, parent=parent, id=wx.ID_ANY, style=wx.LC_REPORT)
		self.controller = tasks
		self.tasks = dict()
		self.InsertColumn(0, "")
		self.Bind(EVT_NOTIFY, self.onNotifyUi)
		self.controller.addObserver(self.onNotifyAsync)
		for thisTask in self.controller.tasks:
			if isinstance(thisTask, task.GoalTask):
				self.newTask(thisTask.goal, thisTask.status)
	def onNotifyAsync(self, queue, msg):
		"""Received a notification on an unknown thread"""
		wx.PostEvent(self, NotifyEvent(msg))
	def onNotifyUi(self, evt):
		"""Received a notification on a UI thread"""
		msg = evt.msg
		if isinstance(msg.task, task.GoalTask):
			if msg.type == task.Notification.ntSTATUS:
				self.newTask(msg.task.goal, msg.value)
		#	elif msg.type == task.Notification.ntDEADASSET:
		#		self.deadTask(msg.value)
	def newTask(self, goal, status):
		"""A new task has been declared.  Assumes UI thread"""
		id = hash(goal)
		if id not in self.tasks:
			pos = self.InsertStringItem(sys.maxint, repr(goal.placeholder))
			self.tasks[id] = pos
			self.SetItemData(pos, id)
	#def deadTask(self, obj):
	#	"""An asset has been removed.  Assumes UI thread"""
	#	if obj in self.tasks:
	#		self.Delete(self.tasks[obj])
	#		del self.tasks[obj]

def frameTest(env, tasks):
	app = wx.App(False)
	frame = wx.Frame(None, wx.ID_ANY, "supermake")
	app.SetTopWindow(frame)
	notebook = wx.Notebook(frame, id=wx.ID_ANY, style=wx.BK_DEFAULT)
	
	tabTasks = wx.Panel(notebook)
	tabTaskList = TasksTab(tabTasks, tasks)
	tabTaskSizer = wx.BoxSizer(wx.VERTICAL)
	tabTaskSizer.Add(tabTaskList, 1, wx.ALL|wx.EXPAND, 5)
	tabTasks.SetSizer(tabTaskSizer)
	notebook.AddPage(tabTasks, "Tasks")
	
	tabAssets = wx.Panel(notebook)
	tabAssetsList = AssetsTab(tabAssets, env)
	tabAssetsSizer = wx.BoxSizer(wx.VERTICAL)
	tabAssetsSizer.Add(tabAssetsList, 1, wx.ALL|wx.EXPAND, 5)
	tabAssets.SetSizer(tabAssetsSizer)
	notebook.AddPage(tabAssets, "Assets")
	
	tabGoals = wx.Panel(notebook)
	tabGoalsList = GoalsTab(tabGoals, tasks)
	tabGoalsSizer = wx.BoxSizer(wx.VERTICAL)
	tabGoalsSizer.Add(tabGoalsList, 1, wx.ALL|wx.EXPAND, 5)
	tabGoals.SetSizer(tabGoalsSizer)
	notebook.AddPage(tabGoals, "Goals")
	
	frame.Layout()
	frame.Show(True)
	app.MainLoop()
