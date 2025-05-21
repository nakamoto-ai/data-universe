class MockLogging:
    def __init__(self):
        self.messages = []
    
    def info(self, message):
        self.messages.append(("INFO", message))
        print(f"INFO: {message}")
        
    def error(self, message):
        self.messages.append(("ERROR", message))
        print(f"ERROR: {message}")
        
    def trace(self, message):
        self.messages.append(("TRACE", message))
        # Don't print trace messages by default to reduce noise
        
    def warning(self, message):
        self.messages.append(("WARNING", message))
        print(f"WARNING: {message}")
    
    def debug(self, message):
        self.messages.append(("DEBUG", message))
        # Don't print debug messages by default to reduce noise

logging = MockLogging()

# Mock other potential bittensor components as needed
class MockMetagraph:
    def __init__(self):
        pass

class MockWallet:
    def __init__(self):
        self.hotkey = MockHotkey()

class MockHotkey:
    def __init__(self):
        self.ss58_address = "mock_ss58_address"