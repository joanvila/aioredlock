
class Lock:

    def __init__(self, resource, lock_identifier, valid=False):
        """
        Initialize a lock with its fields.
        """
        self.resource = resource
        self.id = lock_identifier
        self.valid = valid
