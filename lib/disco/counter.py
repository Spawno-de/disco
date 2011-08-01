from disco.worker import Worker

import sys
class Counter:
    def __init__(self, name):
        self.name = name
    """Simple incrementation of counter"""
    def increment(self):
        self.add(1)
    """Adds value to defined counter"""
    def add(self, value):
        string_value = str(value)
        Worker.send('INC', {'name': self.name, 'value': value})
