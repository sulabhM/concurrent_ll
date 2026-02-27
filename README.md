# Concurrent Linked List

A **lock-free, thread-safe singly linked list** with MVCC (Multi-Version Concurrency Control) semantics. Designed for high-performance concurrent access where multiple threads need consistent point-in-time views of the data.

## Features

- **Lock-free operations** using atomic compare-and-swap (CAS)
- **Snapshot isolation** - readers see a consistent view at a specific point in time
- **Dynamic thread support** - any number of threads can register with a domain
- **Hazard pointer memory safety** - prevents use-after-free during concurrent access
- **Deferred reclamation** - removed nodes are safely freed when no longer referenced

## Quick Start

```c
#include "list.h"

// 1. Create a domain (manages thread state for a group of lists)
ll_domain_t *domain = ll_domain_create(0);  // 0 = default capacity

// 2. Register the current thread
ll_thread_register(domain);

// 3. Initialize a list
ll_head_t list;
ll_init(&list, domain);

// 4. Insert elements
struct my_item *item = malloc(sizeof(*item));
ll_insert_head(&list, item);

// 5. Iterate with snapshot isolation
ll_iterator_t iter;
ll_iterator_begin(&list, &iter);
void *elm;
while ((elm = ll_iterator_next(&iter)) != NULL) {
    // Process element - guaranteed consistent view
}
ll_iterator_end(&iter);

// 6. Cleanup
ll_destroy(&list, free);  // free callback for user elements
ll_thread_unregister(domain);
ll_domain_destroy(domain);
```

## API Reference

### Domain Management

| Function | Description |
|----------|-------------|
| `ll_domain_create(size_t initial_threads)` | Create a new domain. Pass 0 for default capacity. |
| `ll_domain_destroy(ll_domain_t *domain)` | Destroy a domain and free all resources. |

### Thread Registration

| Function | Description |
|----------|-------------|
| `ll_thread_register(ll_domain_t *domain)` | Register current thread with domain. Returns `LL_OK` or error. |
| `ll_thread_unregister(ll_domain_t *domain)` | Unregister current thread from domain. |

### List Lifecycle

| Function | Description |
|----------|-------------|
| `ll_init(ll_head_t *list, ll_domain_t *domain)` | Initialize a list within a domain. |
| `ll_destroy(ll_head_t *list, void (*free_cb)(void *))` | Destroy list and free all elements. |

### Insert/Remove Operations

| Function | Description |
|----------|-------------|
| `ll_insert_head(ll_head_t *list, void *elm)` | Insert element at head. Returns `LL_OK` or error. |
| `ll_remove(ll_head_t *list, void *elm)` | Logically remove element. Returns `LL_OK`, `LL_ERR_NOTFOUND`, or error. |
| `ll_remove_first(ll_head_t *list, void **out)` | Remove and return first visible element. |

### Iteration

| Function | Description |
|----------|-------------|
| `ll_iterator_begin(ll_head_t *list, ll_iterator_t *iter)` | Start iteration with snapshot. |
| `ll_iterator_next(ll_iterator_t *iter)` | Get next visible element, or NULL. |
| `ll_iterator_end(ll_iterator_t *iter)` | End iteration and release snapshot. |
| `ll_iterator_snapshot(ll_iterator_t *iter)` | Get the snapshot version of the iterator. |

### Utility Functions

| Function | Description |
|----------|-------------|
| `ll_is_empty(ll_head_t *list)` | Returns true if list has no visible elements. |
| `ll_contains(ll_head_t *list, void *elm)` | Returns true if element is in list. |
| `ll_count(ll_head_t *list)` | Returns count of visible elements. |
| `ll_reclaim(ll_head_t *list, void (*free_cb)(void *))` | Physically free logically-removed nodes. |

### Error Codes

| Code | Value | Description |
|------|-------|-------------|
| `LL_OK` | 0 | Success |
| `LL_ERR_NOMEM` | -1 | Memory allocation failed |
| `LL_ERR_NOTFOUND` | -2 | Element not found |
| `LL_ERR_INVAL` | -3 | Invalid argument (NULL pointer) |
| `LL_ERR_NOTHREAD` | -4 | Thread not registered with domain |

## Implementation Details

### MVCC Visibility

Each node has two transaction IDs:
- `insert_txn_id` - set when the node is inserted
- `removed_txn_id` - set when the node is logically removed (0 = not removed)

A node is **visible** at snapshot version `S` if:
```
insert_txn_id <= S AND (removed_txn_id == 0 OR removed_txn_id > S)
```

### Hazard Pointers

Memory safety during concurrent access is ensured via hazard pointers:
1. Before accessing a node, a thread "acquires" it by storing the pointer in its hazard slot
2. During reclamation, nodes protected by any thread's hazard pointer are not freed
3. Each thread has 2 hazard pointer slots (for `prev` and `curr` during traversal)

### Deferred Reclamation

Removed nodes are not immediately freed:
1. `ll_remove()` sets `removed_txn_id` (logical deletion)
2. Nodes remain in the list until `ll_reclaim()` is called
3. `ll_reclaim()` checks active snapshots and hazard pointers before freeing

### Lock-Free Insertions

Insertions use a CAS loop:
```c
do {
    old_head = atomic_load(&list->head);
    node->next = old_head;
} while (!atomic_compare_exchange_weak(&list->head, &old_head, node));
```

## Legacy API

For backward compatibility, a macro-based API similar to BSD's `sys/queue.h` is available:

```c
#include "list.h"

struct item {
    int id;
    LL_ENTRY(item) link;  // Embed list linkage
};

LL_HEAD(item_list, item);  // Declare list head type

struct item_list my_list;
ll_init_(&my_list.head, &my_list.commit_id);

// Insert, remove, iterate using ll_insert_head_(), ll_remove_(), etc.
```

See `list.h` for the complete legacy API.

## C++ Usage

For C++ projects, include the compatibility wrapper:

```cpp
#include "list_cxx.h"

struct Item {
    int id;
    LL_ENTRY_CXX(Item, link);  // C++ compatible linkage
};

LL_HEAD_CXX(ItemList, Item);  // C++ compatible list head

int main() {
    ll_domain_t *domain = ll_domain_create(0);
    ll_thread_register(domain);

    ll_head_t list;
    ll_init(&list, domain);

    Item *item = new Item{42};
    ll_insert_head(&list, item);

    // Use ll_snapshot_begin_cxx for type-safe snapshot
    uint64_t snap = ll_snapshot_begin_cxx(&list.commit_id);
    // ... iterate ...
    ll_snapshot_end();

    ll_destroy(&list, [](void *p) { delete static_cast<Item*>(p); });
    ll_thread_unregister(domain);
    ll_domain_destroy(domain);
}
```

The `list_cxx.h` header maps C11 `_Atomic` types to `std::atomic` for C++ compatibility.

## Thread Safety Notes

1. **Always register** before using any list operations
2. **Always end iterators** to release snapshots (prevents memory buildup)
3. **Call reclaim periodically** to free removed nodes
4. Multiple lists can share a domain (recommended for related lists)

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                        ll_domain_t                          │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ Thread States (dynamic array)                        │   │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐            │   │
│  │  │ Thread 0 │ │ Thread 1 │ │ Thread N │  ...       │   │
│  │  │ HP[0,1]  │ │ HP[0,1]  │ │ HP[0,1]  │            │   │
│  │  │ snapshot │ │ snapshot │ │ snapshot │            │   │
│  │  │ retired  │ │ retired  │ │ retired  │            │   │
│  │  └──────────┘ └──────────┘ └──────────┘            │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
        ┌──────────┐    ┌──────────┐    ┌──────────┐
        │ll_head_t │    │ll_head_t │    │ll_head_t │
        │ List A   │    │ List B   │    │ List C   │
        └──────────┘    └──────────┘    └──────────┘
              │
              ▼
    ┌─────────────────┐    ┌─────────────────┐
    │ versioned_node  │───▶│ versioned_node  │───▶ NULL
    │ insert_txn: 1   │    │ insert_txn: 2   │
    │ removed_txn: 0  │    │ removed_txn: 3  │
    │ user_elm ───────┼─┐  │ user_elm ───────┼─┐
    └─────────────────┘ │  └─────────────────┘ │
                        ▼                      ▼
                   [User Data]            [User Data]
```

## Building

```bash
# Clone the repository
git clone https://github.com/yourusername/concurrent_ll.git
cd concurrent_ll

# Build
mkdir build && cd build
cmake ..
make

# Run tests
./concurrent_ll_tests
```

### Build Options

| Option | Default | Description |
|--------|---------|-------------|
| `BUILD_TESTS` | ON | Build the test suite |
| `BUILD_SHARED_LIBS` | OFF | Build shared library instead of static |

### Installing

```bash
cmake --install . --prefix /usr/local
```

## Testing

The test suite includes 43 test cases with ~1400 assertions covering:

- Domain and thread management
- Insert, remove, and iteration operations
- Concurrent access patterns
- Memory reclamation
- MVCC visibility semantics
- Legacy API compatibility

Run tests with verbose output:
```bash
./concurrent_ll_tests -s
```
