import multiprocessing, asyncio, arrow
from rethinkdb import r
from multiprocessing import Pipe
#Hopefully this won't be needed once fucking rethinkdb fucking
#supports fucking asyncio. Fuckers.
def _SpQueryWorker(query,pipe,*args,**kwargs):
    conn = r.connect(*args,**kwargs)
    for event in query.run(conn):
        pipe.send(event)

class SpQuery():
    def __init__(self,query,*args,**kwargs):
        self.pipe,remote=Pipe(duplex=False)
        self.process=multiprocessing.Process(target=_SpQueryWorker, args=(query,remote,*args), kwargs={**kwargs})
        self.process.start()
    async def run(self,pollTime=0.5):
        while True:
            while self.pipe.poll():
                print("reading from pipe")
                yield self.pipe.recv()
            await asyncio.sleep(pollTime)

class cache(dict):
    """
    A dictionary that changes when any key in the db changes
    """
    def __init__(self,query,key):
#        self.size=size
        self.query=query
        self.key=key #What key to use as the value for our cache
        self.atime=dict()#Access time, on cleanup we drop the oldest.

    def __getitem__(self, key):
        if key not in self:
            data = self.query.get_all(key, index=self.key).run(conn)
        self.atime[key]=arrow.utcnow()
        return super()__getitem__(key)

    def connect(self,*args,**kwargs):
        self.connArgs=args
        self.connKwargs=kwargs
        self.conn=r.connect(*args,**kwargs)
        self.loop = asyncio.get_event_loop()
        self.task = loop.create_task(self._watch())
        return self

    def _watch(self):
        events = SpQuery(self.query,*self.connArgs, **self.connKwargs).run()
        async for event in events:
            if getattr(event, self.key, None) in event:
                self[self.key].update(event)

