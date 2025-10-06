"""Cache system"""
import os
import pickle

class Cache:
    """Lib for cache management"""
    def __init__(self, filename='cache.pkl'):
        self.filename = filename
        self.cache = self._load_cache()

    def _load_cache(self):
        if os.path.exists(self.filename):
            with open(self.filename, 'rb') as f:
                return pickle.load(f)
        return {}

    def _save_cache(self):
        with open(self.filename, 'wb') as f:
            pickle.dump(self.cache, f)

    def set(self, key, value):
        """record cache"""
        self.cache[key] = value
        self._save_cache()

    def get(self, key):
        """get values of cache by key"""
        return self.cache.get(key, None)

    def delete(self, key):
        """delete record cache by key"""
        if key in self.cache:
            del self.cache[key]
            self._save_cache()

    def clear(self):
        """delete all records in cache file"""
        self.cache = {}
        self._save_cache()


if __name__ == "__main__":
    cache = Cache()
    print(cache.get('stop'))
