class Node:
    def __init__(self, is_leaf=False):
        self.keys = []
        self.values = []
        self.children = []
        self.is_leaf = is_leaf
        self.next = None

class BPlusTree:
    def __init__(self, degree=3):
        self.root = Node(is_leaf=True)
        self.degree = degree
        self.min_keys = degree - 1
        self.max_keys = 2 * degree - 1

    def insert(self, key, value):
        """Insert a key-value pair into the tree."""
        if len(self.root.keys) == self.max_keys:
            new_root = Node()
            new_root.children.append(self.root)
            self._split_child(new_root, 0)
            self.root = new_root
        self._insert_non_full(self.root, key, value)

    def _insert_non_full(self, node, key, value):
        """Helper function to insert into a non-full node."""
        if node.is_leaf:
            index = 0
            while index < len(node.keys) and key > node.keys[index]:
                index += 1
            node.keys.insert(index, key)
            node.values.insert(index, value)
        else:
            index = 0
            while index < len(node.keys) and key > node.keys[index]:
                index += 1
            if len(node.children[index].keys) == self.max_keys:
                self._split_child(node, index)
                if key > node.keys[index]:
                    index += 1
            self._insert_non_full(node.children[index], key, value)

    def _split_child(self, parent, index):
        """Split a child node when it's full."""
        child = parent.children[index]
        new_node = Node(is_leaf=child.is_leaf)
        
        split_point = len(child.keys) // 2
        middle_key = child.keys[split_point]
        
        if child.is_leaf:
            new_node.keys = child.keys[split_point:]
            new_node.values = child.values[split_point:]
            child.keys = child.keys[:split_point]
            child.values = child.values[:split_point]
            new_node.next = child.next
            child.next = new_node
        else:
            new_node.keys = child.keys[split_point+1:]
            new_node.children = child.children[split_point+1:]
            child.keys = child.keys[:split_point]
            child.children = child.children[:split_point+1]
        
        parent.keys.insert(index, middle_key)
        parent.children.insert(index + 1, new_node)

    def get(self, key):
        """Retrieve a value by key."""
        return self._search(self.root, key)

    def _search(self, node, key):
        """Helper function to search for a key."""
        if node.is_leaf:
            index = 0
            while index < len(node.keys) and key > node.keys[index]:
                index += 1
            if index < len(node.keys) and key == node.keys[index]:
                return node.values[index]
            return None
        else:
            index = 0
            while index < len(node.keys) and key >= node.keys[index]:
                index += 1
            return self._search(node.children[index], key)

    def range_query(self, start_key, end_key):
        """Get all values where start_key <= key <= end_key."""
        results = []
        leaf = self._find_leaf(self.root, start_key)
        
        while leaf:
            for i, key in enumerate(leaf.keys):
                if start_key <= key <= end_key:
                    results.append((key, leaf.values[i]))
                elif key > end_key:
                    return results
            leaf = leaf.next
        return results

    def _find_leaf(self, node, key):
        """Find the leaf node where the key should be located."""
        while not node.is_leaf:
            index = 0
            while index < len(node.keys) and key >= node.keys[index]:
                index += 1
            node = node.children[index]
        return node

    def delete(self, key):
        """Delete a key from the tree."""
        self._delete(self.root, key)
        if not self.root.is_leaf and len(self.root.children) == 1:
            self.root = self.root.children[0]

    def _delete(self, node, key):
        """Helper function for deletion."""
        if node.is_leaf:
            self._delete_from_leaf(node, key)
        else:
            self._delete_from_internal(node, key)

    def _delete_from_leaf(self, node, key):
        """Delete a key from a leaf node."""
        if key in node.keys:
            index = node.keys.index(key)
            node.keys.pop(index)
            node.values.pop(index)
        else:
            raise KeyError(f"Key {key} not found in the tree")

    def _delete_from_internal(self, node, key):
        """Delete a key from an internal node."""
        index = 0
        while index < len(node.keys) and key > node.keys[index]:
            index += 1
        
        if index < len(node.keys) and key == node.keys[index]:
            self._delete_internal_key(node, index)
        else:
            child = node.children[index]
            if len(child.keys) == self.min_keys:
                self._fix_child(node, index)
                if index > len(node.keys):
                    index -= 1
            self._delete(child, key)

    def _delete_internal_key(self, node, index):
        """Delete a key from an internal node and handle rebalancing."""
        # Implementation would go here
        pass

    def _fix_child(self, node, index):
        """Rebalance the tree after deletion."""
        # Implementation would go here
        pass