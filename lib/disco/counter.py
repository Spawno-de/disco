import sys
class Counter:
    def __init__(self, name):
        self.name = name
    def increment(self):
        print >> sys.stderr, "INC", len(self.name)+2, "\""+self.name+"\""
