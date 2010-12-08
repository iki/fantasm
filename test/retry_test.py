""" Test machine for built-in TaskRetryOptions. """

class State1(object):
    
    def execute(self, context, obj):
        return 'ok'
        
class State2(object):
    
    def execute(self, context, obj):
        raise Exception()
