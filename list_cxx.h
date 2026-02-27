/**
 * C++ Compatibility Header for Concurrent Linked List
 *
 * This header provides C++ compatible wrappers for the concurrent linked list.
 * Include this header instead of list.h when using from C++ code.
 *
 * This supports both the new domain-based API and the legacy API for
 * backward compatibility.
 */

#ifndef LIST_CXX_H
#define LIST_CXX_H

#include <atomic>
#include <cstddef>
#include <cstdint>

#ifdef __cplusplus

/* ============== Error Codes ============== */

#define LL_OK           0
#define LL_ERR_NOMEM   -1
#define LL_ERR_NOTFOUND -2
#define LL_ERR_NOTHREAD -3
#define LL_ERR_INVAL   -4
#define LL_ERR_FULL    -5

/* ============== C++ Type Definitions ============== */

/*
 * C++ compatible atomic types that match the C implementation.
 * These must have the same memory layout as the C types.
 */
typedef std::atomic<uint64_t> ll_commit_id_t;
typedef std::atomic<uintptr_t> ll_atomic_uintptr_t;

/*
 * Embed this in your struct to make it listable (legacy API).
 * Example: struct item { int id; LL_ENTRY_CXX(item, link); };
 */
#define LL_ENTRY_CXX(type, name) ll_atomic_uintptr_t name

/*
 * Declare a list head type for C++ (legacy API).
 * Example: LL_HEAD_CXX(list_head, item) my_list;
 */
#define LL_HEAD_CXX(name, type)                   \
    struct name {                                 \
        ll_atomic_uintptr_t head;                 \
        ll_commit_id_t commit_id;                 \
        void (*free_cb)(struct type *);           \
    }

/* Opaque domain handle. */
struct ll_domain;
typedef ll_domain ll_domain_t;

/* List head structure - matches C layout. */
struct ll_head_t {
    ll_atomic_uintptr_t head;
    ll_commit_id_t commit_id;
    ll_domain_t *domain;
};

/* Iterator structure - matches C layout. */
struct ll_iterator_t {
    ll_head_t *list;
    uint64_t snapshot;
    void *current_node;
};

/* ============== External C Functions ============== */

extern "C" {

/* Domain management. */
ll_domain_t *ll_domain_create(size_t initial_threads);
void ll_domain_destroy(ll_domain_t *domain);
int ll_thread_register(ll_domain_t *domain);
void ll_thread_unregister(ll_domain_t *domain);

/* New API. */
int ll_init(ll_head_t *list, ll_domain_t *domain);
void ll_destroy(ll_head_t *list, void (*free_cb)(void *));
int ll_insert_head(ll_head_t *list, void *elm);
int ll_remove(ll_head_t *list, void *elm);
int ll_remove_first(ll_head_t *list, void **out_elm);
int ll_iterator_begin(ll_head_t *list, ll_iterator_t *iter);
void *ll_iterator_next(ll_iterator_t *iter);
void ll_iterator_end(ll_iterator_t *iter);
uint64_t ll_iterator_snapshot(const ll_iterator_t *iter);
bool ll_is_empty(ll_head_t *list);
bool ll_contains(ll_head_t *list, const void *elm);
size_t ll_count(ll_head_t *list);
void ll_reclaim(ll_head_t *list, void (*free_cb)(void *));

/* Legacy API (deprecated but still supported). */
void ll_init_(void *head, void *commit_id);
void ll_insert_head_(void *head, void *commit_id, void *elm);
void *ll_remove_head_(void *head, void *commit_id);
int ll_remove_(void *head, void *commit_id, void (*free_cb)(void *), void *elm);
uint64_t ll_snapshot_begin(void *commit_id);
void ll_snapshot_end(void);
void *ll_snapshot_first_(void *head, void *commit_id, uint64_t snapshot_version);
void *ll_snapshot_next_(void *head, void *commit_id, uint64_t snapshot_version, const void *elm);
void ll_reclaim_(void *head, void *commit_id, void (*free_cb)(void *));

}

/* ============== Legacy C++ Wrapper Functions ============== */

/*
 * Legacy list head structure for backward compatibility with existing tests.
 */
struct ll_legacy_head {
    ll_atomic_uintptr_t head;
    ll_commit_id_t commit_id;
    void (*free_cb)(void *);
};

inline void
ll_init(ll_atomic_uintptr_t *head, ll_commit_id_t *commit_id)
{
    ll_init_(head, commit_id);
}

inline void
ll_insert_head(ll_atomic_uintptr_t *head, ll_commit_id_t *commit_id, void *elm)
{
    ll_insert_head_(head, commit_id, elm);
}

inline void *
ll_remove_head(ll_atomic_uintptr_t *head, ll_commit_id_t *commit_id)
{
    return ll_remove_head_(head, commit_id);
}

inline int
ll_remove(ll_atomic_uintptr_t *head, ll_commit_id_t *commit_id, void (*free_cb)(void *), void *elm)
{
    return ll_remove_(head, commit_id, free_cb, elm);
}

inline uint64_t
ll_snapshot_begin_cxx(ll_commit_id_t *commit_id)
{
    return ll_snapshot_begin(commit_id);
}

inline void *
ll_snapshot_first(ll_atomic_uintptr_t *head, ll_commit_id_t *commit_id, uint64_t snapshot_version)
{
    return ll_snapshot_first_(head, commit_id, snapshot_version);
}

inline void *
ll_snapshot_next(
  ll_atomic_uintptr_t *head, ll_commit_id_t *commit_id, uint64_t snapshot_version, const void *elm)
{
    return ll_snapshot_next_(head, commit_id, snapshot_version, elm);
}

inline void
ll_reclaim(ll_atomic_uintptr_t *head, ll_commit_id_t *commit_id, void (*free_cb)(void *))
{
    ll_reclaim_(head, commit_id, free_cb);
}

#endif /* __cplusplus */

#endif /* LIST_CXX_H */

