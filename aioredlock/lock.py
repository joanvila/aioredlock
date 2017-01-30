
class Lock:

    def __init__(self, resource, valid=False):
        """
        Initialize a lock with its fields.
        """
        self.resource = resource
        self.valid = valid

    async def unlock(self):
        """
        Release the lock setting it's validity to false.
        """
        self.valid = False
