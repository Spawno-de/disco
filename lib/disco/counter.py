"""
:mod:`disco.counter` -- Disco Counter
=======================================

The :mod:`disco.counter` module provides interface for
creating and updating counters. Counters can be used for sanity checking.
They are updated every time when some event occures.
If job is terminated due to errors counters are NOT reset.
"""

from disco.worker import Worker
import re

class Counter:
    """
    The :class:`Counter` object provides an interface to create and
    update counters. Every counter has its name. Creating more counters
    with the same name in one job is equivalent to creating one counter
    whith that name which holds sum of those counters.
    Counters are not shared between different jobs.
    """

    def __init__(self, name):
        if re.match(r"[A-Za-z0-9_]*$", name):
            self.name = name
        else:
            raise ValueError("Counter name must match: [A-Za-z0-9_]")
            
    
    def increment(self):
        """Simple incrementation of counter"""
        self.add(1)
    
    def add(self, value):
        """Adds value to defined counter"""
        string_value = str(value)
        Worker.send('INC', {'name': self.name, 'value': value})
