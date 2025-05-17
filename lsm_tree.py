import json
import os
from collections import OrderedDict
import time
import glob

class LSMTree:
    def __init__(self, max_memtable_size=1000, sstable_dir="sstables"):
        self.memtable = OrderedDict()
        self.sstables = []  # List of sstable file paths in order from newest to oldest
        self.max_memtable_size = max_memtable_size
        self.wal_file = "wal.log"
        self.sstable_dir = sstable_dir
        self._initialize_storage()
        self._recover_from_wal()
        self._load_existing_sstables()

    def _initialize_storage(self):
        """Create necessary directories if they don't exist"""
        if not os.path.exists(self.sstable_dir):
            os.makedirs(self.sstable_dir)

    def _recover_from_wal(self):
        """Recover data from Write-Ahead Log (WAL) after crash"""
        if os.path.exists(self.wal_file):
            try:
                with open(self.wal_file, 'r') as f:
                    for line in f:
                        try:
                            key, value = line.strip().split(':', 1)
                            self.memtable[key] = value
                        except ValueError:
                            continue  # Skip malformed lines
            except IOError:
                pass  # Couldn't read WAL, start fresh

    def _load_existing_sstables(self):
        """Load existing SSTables from disk"""
        sstable_files = sorted(glob.glob(os.path.join(self.sstable_dir, 'sstable_*.json')), reverse=True)
        self.sstables = sstable_files

    def insert(self, key, value):
        """Insert a key-value pair into the LSM Tree"""
        if not isinstance(key, str):
            key = str(key)
        if not isinstance(value, str):
            value = str(value)

        # Write to WAL first for durability
        try:
            with open(self.wal_file, 'a') as f:
                f.write(f"{key}:{value}\n")
        except IOError as e:
            raise IOError(f"Failed to write to WAL: {str(e)}")

        # Update memtable
        self.memtable[key] = value

        # Check if we need to flush to disk
        if len(self.memtable) >= self.max_memtable_size:
            self._flush_memtable()

    def _flush_memtable(self):
        """Flush the current memtable to disk as a new SSTable"""
        if not self.memtable:
            return

        timestamp = int(time.time() * 1000)  # Millisecond precision
        sstable_name = os.path.join(self.sstable_dir, f"sstable_{timestamp}.json")
        
        try:
            # Write new SSTable
            with open(sstable_name, 'w') as f:
                json.dump(self.memtable, f)
            
            # Update sstables list (newest first)
            self.sstables.insert(0, sstable_name)
            
            # Clear memtable and WAL
            self.memtable = OrderedDict()
            try:
                os.remove(self.wal_file)
            except OSError:
                pass  # WAL might not exist
            
        except IOError as e:
            raise IOError(f"Failed to flush memtable to SSTable: {str(e)}")

    def get(self, key):
        """Retrieve a value by key (returns None if not found)"""
        if not isinstance(key, str):
            key = str(key)

        # Check memtable first (most recent data)
        if key in self.memtable:
            return self.memtable[key]
        
        # Check SSTables from newest to oldest
        for sstable in self.sstables:
            try:
                with open(sstable, 'r') as f:
                    data = json.load(f)
                    if key in data:
                        return data[key]
            except (IOError, json.JSONDecodeError):
                continue  # Skip corrupt SSTables
        
        return None

    def range_query(self, start_key, end_key):
        """Get all values where start_key <= key <= end_key"""
        results = []
        
        # Convert keys to strings if they aren't already
        if not isinstance(start_key, str):
            start_key = str(start_key)
        if not isinstance(end_key, str):
            end_key = str(end_key)

        # Check memtable first
        for key, value in self.memtable.items():
            if start_key <= key <= end_key:
                results.append((key, value))  # Return (key, value) pairs
        
        # Check SSTables from newest to oldest
        for sstable in self.sstables:
            try:
                with open(sstable, 'r') as f:
                    data = json.load(f)
                    for key, value in data.items():
                        if start_key <= key <= end_key:
                            results.append((key, value))
            except (IOError, json.JSONDecodeError):
                continue  # Skip corrupt SSTables
        
        return results

    def compact(self):
        """Perform compaction to merge and reduce SSTables"""
        if len(self.sstables) <= 1:
            return  # Nothing to compact

        # Create a new merged SSTable
        merged = OrderedDict()
        for sstable in reversed(self.sstables):  # Process from oldest to newest
            try:
                with open(sstable, 'r') as f:
                    data = json.load(f)
                    merged.update(data)
            except (IOError, json.JSONDecodeError):
                continue  # Skip corrupt SSTables

        # Write new compacted SSTable
        timestamp = int(time.time() * 1000)
        new_sstable = os.path.join(self.sstable_dir, f"sstable_compact_{timestamp}.json")
        with open(new_sstable, 'w') as f:
            json.dump(merged, f)

        # Remove old SSTables
        for sstable in self.sstables:
            try:
                os.remove(sstable)
            except OSError:
                pass

        # Update sstables list with just the new compacted one
        self.sstables = [new_sstable]

    def clear(self):
        """Clear all data (for testing/reset purposes)"""
        self.memtable = OrderedDict()
        self.sstables = []
        try:
            os.remove(self.wal_file)
        except OSError:
            pass
        for sstable in glob.glob(os.path.join(self.sstable_dir, 'sstable_*.json')):
            try:
                os.remove(sstable)
            except OSError:
                pass