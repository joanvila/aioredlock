
class Lock:

    def __init__(self, valid=False):
        """
        Initialize a lock with its fields.
        """
        self.valid = valid

    async def unlock(self):
        """
        Release the lock setting it's validity to false.
        """
        self.valid = False
