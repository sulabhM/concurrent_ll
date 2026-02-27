/**
 * Concurrent Linked List - Public API
 *
 * Lock-free, thread-safe singly linked list with MVCC (Multi-Version
 * Concurrency Control) semantics. Features:
 *
 * - Lock-free operations using atomic CAS
 * - Snapshot isolation for consistent traversals
 * - Safe memory reclamation via hazard pointers
 * - Dynamic thread support (no fixed thread limit)
 * - Per-list state (multiple independent lists supported)
 *
 * Visibility rule: A node is visible at snapshot S if:
 *   insert_txn_id <= S AND (removed_txn_id == 0 OR removed_txn_id > S)
 *
 * Usage:
 *   1. Create a domain with ll_domain_create() - shared by related lists
 *   2. Initialize lists with ll_init()
 *   3. Register threads with ll_thread_register() before using any list
 *   4. Use ll_insert_head(), ll_remove(), ll_snapshot_*() for operations
 *   5. Unregister threads with ll_thread_unregister() when done
 *   6. Destroy lists with ll_destroy() and domain with ll_domain_destroy()
 */

#ifndef LIST_H
#define LIST_H

#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============== Error Codes ============== */

#define LL_OK           0   /* Success */
#define LL_ERR_NOMEM   -1   /* Memory allocation failed */
#define LL_ERR_NOTFOUND -2  /* Element not found */
#define LL_ERR_NOTHREAD -3  /* Thread not registered */
#define LL_ERR_INVAL   -4   /* Invalid argument */
#define LL_ERR_FULL    -5   /* Resource limit reached */

/* ============== Types ============== */

/* Opaque domain handle - manages hazard pointers for a group of lists. */
typedef struct ll_domain ll_domain_t;

/* Commit ID type (atomic 64-bit counter). */
typedef _Atomic(uint64_t) ll_commit_id_t;

/* List head structure - one per list. */
typedef struct ll_head {
    atomic_uintptr_t head;      /* Pointer to first versioned_node */
    ll_commit_id_t commit_id;   /* Monotonic transaction counter */
    ll_domain_t *domain;        /* Associated hazard pointer domain */
} ll_head_t;

/* Iterator for efficient traversal (avoids O(N²) issue). */
typedef struct ll_iterator {
    ll_head_t *list;            /* List being traversed */
    uint64_t snapshot;          /* Snapshot version for this traversal */
    void *current_node;         /* Internal: current versioned_node pointer */
} ll_iterator_t;

/* ============== Domain Management ============== */

/*
 * Create a hazard pointer domain. All lists sharing the same domain
 * can safely be used by threads registered with that domain.
 *
 * @param initial_threads  Initial capacity for threads (will grow as needed)
 * @return Domain pointer on success, NULL on allocation failure
 */
ll_domain_t *ll_domain_create(size_t initial_threads);

/*
 * Destroy a hazard pointer domain. All lists using this domain must be
 * destroyed first, and all threads must be unregistered.
 *
 * @param domain  Domain to destroy
 */
void ll_domain_destroy(ll_domain_t *domain);

/*
 * Register the current thread with a domain. Must be called before
 * using any list operations on lists in this domain.
 *
 * @param domain  Domain to register with
 * @return LL_OK on success, LL_ERR_NOMEM if allocation fails
 */
int ll_thread_register(ll_domain_t *domain);

/*
 * Unregister the current thread from a domain. Call when the thread
 * is done using lists in this domain.
 *
 * @param domain  Domain to unregister from
 */
void ll_thread_unregister(ll_domain_t *domain);

/* ============== List Lifecycle ============== */

/*
 * Initialize a list head.
 *
 * @param list    List head to initialize
 * @param domain  Domain for hazard pointer management
 * @return LL_OK on success, LL_ERR_INVAL if arguments are NULL
 */
int ll_init(ll_head_t *list, ll_domain_t *domain);

/*
 * Destroy a list. Frees all remaining nodes. The list must be quiescent
 * (no concurrent operations).
 *
 * @param list     List to destroy
 * @param free_cb  Callback to free user elements (may be NULL)
 */
void ll_destroy(ll_head_t *list, void (*free_cb)(void *));

/* ============== Insert Operations ============== */

/*
 * Insert an element at the head of the list.
 *
 * @param list  List to insert into
 * @param elm   User element to insert (must not be NULL)
 * @return LL_OK on success, LL_ERR_NOMEM on allocation failure,
 *         LL_ERR_NOTHREAD if thread not registered
 */
int ll_insert_head(ll_head_t *list, void *elm);

/* ============== Remove Operations ============== */

/*
 * Logically remove an element from the list. The element remains in the
 * list but is marked as removed and will not be visible to new snapshots.
 * Physical removal happens during ll_reclaim().
 *
 * @param list  List to remove from
 * @param elm   User element to remove
 * @return LL_OK on success, LL_ERR_NOTFOUND if element not in list,
 *         LL_ERR_NOTHREAD if thread not registered
 */
int ll_remove(ll_head_t *list, void *elm);

/*
 * Remove and return the first visible element from the list.
 * This physically unlinks and frees the internal node immediately.
 *
 * @param list     List to remove from
 * @param out_elm  Output: the removed user element (if any)
 * @return LL_OK on success (element stored in *out_elm),
 *         LL_ERR_NOTFOUND if list is empty,
 *         LL_ERR_NOTHREAD if thread not registered
 */
int ll_remove_first(ll_head_t *list, void **out_elm);

/* ============== Snapshot & Traversal ============== */

/*
 * Begin an iterator for snapshot-consistent traversal. The iterator
 * provides a consistent view of the list at the time of creation.
 * Must be paired with ll_iterator_end().
 *
 * @param list  List to iterate
 * @param iter  Iterator structure to initialize
 * @return LL_OK on success, LL_ERR_NOTHREAD if thread not registered
 */
int ll_iterator_begin(ll_head_t *list, ll_iterator_t *iter);

/*
 * Get the next visible element from the iterator.
 *
 * @param iter  Iterator from ll_iterator_begin()
 * @return Next visible element, or NULL if no more elements
 */
void *ll_iterator_next(ll_iterator_t *iter);

/*
 * End iteration and release the snapshot.
 *
 * @param iter  Iterator to end
 */
void ll_iterator_end(ll_iterator_t *iter);

/*
 * Get the snapshot version from an iterator (for advanced use).
 *
 * @param iter  Active iterator
 * @return Snapshot version
 */
uint64_t ll_iterator_snapshot(const ll_iterator_t *iter);

/* ============== Utility Functions ============== */

/*
 * Check if the list is empty (no visible elements at current snapshot).
 *
 * @param list  List to check
 * @return true if empty, false otherwise
 */
bool ll_is_empty(ll_head_t *list);

/*
 * Check if an element is in the list (visible at current snapshot).
 *
 * @param list  List to search
 * @param elm   Element to find
 * @return true if found and visible, false otherwise
 */
bool ll_contains(ll_head_t *list, const void *elm);

/*
 * Count visible elements in the list.
 *
 * @param list  List to count
 * @return Number of visible elements
 */
size_t ll_count(ll_head_t *list);

/* ============== Memory Reclamation ============== */

/*
 * Reclaim logically removed nodes. Physically unlinks nodes that are
 * not visible to any active snapshot and frees them.
 *
 * @param list     List to reclaim from
 * @param free_cb  Callback to free user elements (may be NULL)
 */
void ll_reclaim(ll_head_t *list, void (*free_cb)(void *));

/* ============== Legacy API (Deprecated) ============== */

/*
 * The following functions maintain backward compatibility but are deprecated.
 * They use a global default domain with fixed thread limit.
 * New code should use the domain-based API above.
 */

/* Initialize using global domain (deprecated). */
void ll_init_(atomic_uintptr_t *head, ll_commit_id_t *commit_id);

/* Insert at head using global domain (deprecated). */
void ll_insert_head_(atomic_uintptr_t *head, ll_commit_id_t *commit_id, void *elm);

/* Remove first visible element using global domain (deprecated). */
void *ll_remove_head_(atomic_uintptr_t *head, ll_commit_id_t *commit_id);

/* Logical remove using global domain (deprecated). */
int ll_remove_(atomic_uintptr_t *head, ll_commit_id_t *commit_id,
               void (*free_cb)(void *), void *elm);

/* Begin snapshot using global domain (deprecated). */
uint64_t ll_snapshot_begin(ll_commit_id_t *commit_id);

/* End snapshot using global domain (deprecated). */
void ll_snapshot_end(void);

/* Get first visible element at snapshot (deprecated). */
void *ll_snapshot_first_(atomic_uintptr_t *head, ll_commit_id_t *commit_id,
                         uint64_t snapshot_version);

/* Get next visible element at snapshot (deprecated). */
void *ll_snapshot_next_(atomic_uintptr_t *head, ll_commit_id_t *commit_id,
                        uint64_t snapshot_version, const void *elm);

/* Reclaim removed nodes using global domain (deprecated). */
void ll_reclaim_(atomic_uintptr_t *head, ll_commit_id_t *commit_id,
                 void (*free_cb)(void *));

#ifdef __cplusplus
}
#endif

#endif /* LIST_H */
