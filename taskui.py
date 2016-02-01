import wx, wx.gizmos, threading
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
		self.AddColumn("Main column")
		self.root = self.AddRoot(text="Hidden Root")
		self.SetMainColumn(0) # the one with the tree in it...
		self.Bind(EVT_NOTIFY, self.onNotifyUi)
		env.addObserver(self.onNotifyAsync)
		for type in env.assetsByType:
			for asset in env.assetsByType[type]:
				self.newAsset(asset)
	def onNotifyAsync(self, queue, msg):
		"""Received a notification on an unknown thread"""
		print '[onNotifyAsync] Received message: ', msg
		wx.PostEvent(self, NotifyEvent(msg))
	def onNotifyUi(self, evt):
		"""Received a notification on a UI thread"""
		msg = evt.msg
		print '[onNotifyUi] Received message: ', msg
		if msg.type == task.Notification.ntNEWASSET:
			self.newAsset(msg.value)
		elif msg.type == task.Notification.ntDEADASSET:
			self.deadAsset(msg.value)
	def newAsset(self, obj):
		"""A new asset has been declared.  Assumes UI thread"""
		if obj not in self.assets:
			self.assets[obj] = self.AppendItem(self.root, text=repr(obj))
	def deadAsset(self, obj):
		"""An asset has been removed.  Assumes UI thread"""
		if obj in self.assets:
			self.Delete(self.assets[obj])
			del self.assets[obj]

def frameTest(env, tasks):
	app = wx.App(False)
	frame = wx.Frame(None, wx.ID_ANY, "supermake")
	app.SetTopWindow(frame)
	notebook = wx.Notebook(frame, id=wx.ID_ANY, style=wx.BK_DEFAULT)
	
	tabTasks = wx.Panel(notebook)
	tabTaskList = wx.ListCtrl(tabTasks, id=wx.ID_ANY, style=wx.LC_LIST)
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
	tabGoalsList = wx.ListCtrl(tabGoals, id=wx.ID_ANY, style=wx.LC_LIST)
	tabGoalsSizer = wx.BoxSizer(wx.VERTICAL)
	tabGoalsSizer.Add(tabGoalsList, 1, wx.ALL|wx.EXPAND, 5)
	tabGoals.SetSizer(tabGoalsSizer)
	notebook.AddPage(tabGoals, "Goals")
	
	frame.Layout()
	frame.Show(True)
	app.MainLoop()
