from disco.worker import Worker

import sys
class Counter:
    def __init__(self, name):
        self.name = name
    def increment(self):
        self.add(1)
    def add(self, value):
        string_value = str(value)
        Worker.send('INC', {'name': self.name, 'value': value})
        #print >> sys.stderr, "INC", len(self.name)+len(string_value)+2, "\""+self.name+"\"", string_value
