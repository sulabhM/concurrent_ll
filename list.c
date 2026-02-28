/**
 * Concurrent Linked List Implementation
 *
 * Lock-free, thread-safe singly linked list with MVCC semantics.
 *
 * Visibility rule: A node is visible at snapshot S if:
 *   insert_txn_id < S AND (removed_txn_id == 0 OR removed_txn_id > S)
 *
 * Memory safety is ensured via hazard pointers:
 * - Each thread can protect up to HP_SLOTS_PER_THREAD pointers
 * - Protected pointers won't be freed during reclamation
 * - Threads must register before using any list operations
 */

#include "list.h"

#include <assert.h>
#include <stdalign.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* ============== Internal Constants ============== */

#define HP_SLOTS_PER_THREAD 2  /* prev and curr during traversal */
#define INITIAL_HP_CAPACITY 16

/* ============== Internal Structures ============== */

/* Versioned wrapper: list chains these; each holds user element + version ids. */
typedef struct versioned_node {
    void *user_elm;
    uint64_t insert_txn_id;
    _Atomic uint64_t removed_txn_id; /* 0 = not removed */
    atomic_uintptr_t next;
} versioned_node_t;

/* Per-thread state within a domain. */
typedef struct ll_thread_state {
    _Atomic(void *) hazard_ptrs[HP_SLOTS_PER_THREAD];
    _Atomic uint64_t active_snapshot;
    versioned_node_t *retired_list;    /* Thread-local retired nodes */
    _Atomic bool in_use;               /* Is this slot taken? */
} ll_thread_state_t;

/* Hazard pointer domain - manages thread state for a group of lists. */
struct ll_domain {
    ll_thread_state_t **threads;       /* Dynamic array of thread state pointers */
    _Atomic size_t thread_count;       /* Number of allocated slots */
    _Atomic size_t capacity;           /* Current capacity */
    _Atomic(struct ll_domain *) next;  /* For global domain list (cleanup) */
    atomic_flag resize_lock;           /* Lock for resizing threads array */
};

/* Thread-local: pointer to this thread's state in the domain. */
static _Thread_local ll_thread_state_t *tls_thread_state = NULL;
static _Thread_local ll_domain_t *tls_domain = NULL;

/* ============== Helper Functions ============== */

static inline versioned_node_t *ptr_unmask(uintptr_t u)
{
    /* Strip any mark bits (lowest bit reserved for future use). */
    return (versioned_node_t *)(u & ~(uintptr_t)1UL);
}

static inline bool node_visible(versioned_node_t *w, uint64_t snapshot)
{
    if (!w)
        return false;
    uint64_t rid = atomic_load_explicit(&w->removed_txn_id, memory_order_acquire);
    return w->insert_txn_id < snapshot && (rid == 0 || rid > snapshot);
}

/* ============== Domain Management ============== */

ll_domain_t *ll_domain_create(size_t initial_threads)
{
    if (initial_threads == 0)
        initial_threads = INITIAL_HP_CAPACITY;

    ll_domain_t *domain = (ll_domain_t *)calloc(1, sizeof(ll_domain_t));
    if (!domain)
        return NULL;

    domain->threads = (ll_thread_state_t **)calloc(initial_threads,
                                                    sizeof(ll_thread_state_t *));
    if (!domain->threads) {
        free(domain);
        return NULL;
    }

    atomic_store(&domain->capacity, initial_threads);
    atomic_store(&domain->thread_count, 0);
    atomic_flag_clear(&domain->resize_lock);

    return domain;
}

void ll_domain_destroy(ll_domain_t *domain)
{
    if (!domain)
        return;

    size_t cap = atomic_load(&domain->capacity);
    for (size_t i = 0; i < cap; i++) {
        if (domain->threads[i]) {
            /* Free any remaining retired nodes. */
            versioned_node_t *node = domain->threads[i]->retired_list;
            while (node) {
                versioned_node_t *next = ptr_unmask(
                    atomic_load_explicit(&node->next, memory_order_relaxed));
                free(node);
                node = next;
            }
            free(domain->threads[i]);
        }
    }
    free(domain->threads);
    free(domain);
}

/* Grow the thread array if needed. Returns 0 on success. */
static int domain_grow(ll_domain_t *domain, size_t needed)
{
    size_t cap = atomic_load_explicit(&domain->capacity, memory_order_acquire);
    if (needed <= cap)
        return 0;

    /* Acquire resize lock. */
    while (atomic_flag_test_and_set_explicit(&domain->resize_lock,
                                              memory_order_acquire)) {
        /* Spin - could add backoff here. */
    }

    /* Re-check after acquiring lock. */
    cap = atomic_load_explicit(&domain->capacity, memory_order_acquire);
    if (needed <= cap) {
        atomic_flag_clear_explicit(&domain->resize_lock, memory_order_release);
        return 0;
    }

    size_t new_cap = cap * 2;
    while (new_cap < needed)
        new_cap *= 2;

    ll_thread_state_t **new_threads = (ll_thread_state_t **)calloc(
        new_cap, sizeof(ll_thread_state_t *));
    if (!new_threads) {
        atomic_flag_clear_explicit(&domain->resize_lock, memory_order_release);
        return LL_ERR_NOMEM;
    }

    /* Copy existing pointers. */
    memcpy(new_threads, domain->threads, cap * sizeof(ll_thread_state_t *));

    ll_thread_state_t **old = domain->threads;
    domain->threads = new_threads;
    atomic_store_explicit(&domain->capacity, new_cap, memory_order_release);

    atomic_flag_clear_explicit(&domain->resize_lock, memory_order_release);

    /* Free old array (safe because we only added capacity). */
    free(old);
    return 0;
}

int ll_thread_register(ll_domain_t *domain)
{
    if (!domain)
        return LL_ERR_INVAL;

    /* Already registered with this domain? */
    if (tls_domain == domain && tls_thread_state != NULL)
        return LL_OK;

    /* Find or create a slot. */
    size_t cap = atomic_load_explicit(&domain->capacity, memory_order_acquire);

    /* First, try to find an existing free slot. */
    for (size_t i = 0; i < cap; i++) {
        ll_thread_state_t *slot = domain->threads[i];
        if (slot) {
            bool expected = false;
            if (atomic_compare_exchange_strong(&slot->in_use, &expected, true)) {
                tls_thread_state = slot;
                tls_domain = domain;
                return LL_OK;
            }
        }
    }

    /* No free slot, allocate a new one. */
    size_t idx = atomic_fetch_add(&domain->thread_count, 1);
    if (idx >= cap) {
        int err = domain_grow(domain, idx + 1);
        if (err != 0) {
            atomic_fetch_sub(&domain->thread_count, 1);
            return err;
        }
    }

    ll_thread_state_t *state = (ll_thread_state_t *)calloc(1, sizeof(ll_thread_state_t));
    if (!state) {
        atomic_fetch_sub(&domain->thread_count, 1);
        return LL_ERR_NOMEM;
    }

    atomic_store(&state->in_use, true);
    atomic_store(&state->active_snapshot, (uint64_t)0);
    for (int i = 0; i < HP_SLOTS_PER_THREAD; i++)
        atomic_store(&state->hazard_ptrs[i], NULL);
    state->retired_list = NULL;

    domain->threads[idx] = state;
    tls_thread_state = state;
    tls_domain = domain;

    return LL_OK;
}

void ll_thread_unregister(ll_domain_t *domain)
{
    if (!domain || tls_domain != domain || !tls_thread_state)
        return;

    /* Clear hazard pointers and snapshot. */
    for (int i = 0; i < HP_SLOTS_PER_THREAD; i++)
        atomic_store(&tls_thread_state->hazard_ptrs[i], NULL);
    atomic_store(&tls_thread_state->active_snapshot, (uint64_t)0);

    /* Mark slot as available for reuse. */
    atomic_store(&tls_thread_state->in_use, false);

    tls_thread_state = NULL;
    tls_domain = NULL;
}
/* ============== Hazard Pointer Helpers ============== */

static inline void hp_acquire(ll_thread_state_t *state, int slot, void *p)
{
    assert(state != NULL);
    assert(slot >= 0 && slot < HP_SLOTS_PER_THREAD);
    atomic_store_explicit(&state->hazard_ptrs[slot], p, memory_order_release);
}

static inline void hp_release(ll_thread_state_t *state, int slot)
{
    assert(state != NULL);
    assert(slot >= 0 && slot < HP_SLOTS_PER_THREAD);
    atomic_store_explicit(&state->hazard_ptrs[slot], NULL, memory_order_release);
}

static inline void hp_release_all(ll_thread_state_t *state)
{
    if (!state)
        return;
    for (int i = 0; i < HP_SLOTS_PER_THREAD; i++)
        atomic_store_explicit(&state->hazard_ptrs[i], NULL, memory_order_release);
}

/* Check if any thread in the domain has a hazard pointer to p. */
static bool any_hp_equals(ll_domain_t *domain, void *p)
{
    if (!domain)
        return false;

    size_t count = atomic_load_explicit(&domain->thread_count, memory_order_acquire);
    for (size_t i = 0; i < count; i++) {
        ll_thread_state_t *state = domain->threads[i];
        if (!state)
            continue;
        for (int j = 0; j < HP_SLOTS_PER_THREAD; j++) {
            if (atomic_load_explicit(&state->hazard_ptrs[j], memory_order_acquire) == p)
                return true;
        }
    }
    return false;
}

/* Find the minimum active snapshot version across all threads. */
static uint64_t min_active_snapshot(ll_domain_t *domain)
{
    if (!domain)
        return UINT64_MAX;

    uint64_t min = UINT64_MAX;
    size_t count = atomic_load_explicit(&domain->thread_count, memory_order_acquire);
    for (size_t i = 0; i < count; i++) {
        ll_thread_state_t *state = domain->threads[i];
        if (!state)
            continue;
        uint64_t v = atomic_load_explicit(&state->active_snapshot, memory_order_acquire);
        if (v != 0 && v < min)
            min = v;
    }
    return min;
}

/* ============== List Lifecycle ============== */

int ll_init(ll_head_t *list, ll_domain_t *domain)
{
    if (!list || !domain)
        return LL_ERR_INVAL;

    atomic_store_explicit(&list->head, (uintptr_t)0, memory_order_release);
    atomic_store_explicit(&list->commit_id, 1, memory_order_release);
    list->domain = domain;

    return LL_OK;
}

void ll_destroy(ll_head_t *list, void (*free_cb)(void *))
{
    if (!list)
        return;

    /* Free all nodes (assumes no concurrent access). */
    versioned_node_t *curr = ptr_unmask(
        atomic_load_explicit(&list->head, memory_order_acquire));
    while (curr) {
        versioned_node_t *next = ptr_unmask(
            atomic_load_explicit(&curr->next, memory_order_acquire));
        if (free_cb)
            free_cb(curr->user_elm);
        free(curr);
        curr = next;
    }
    atomic_store_explicit(&list->head, (uintptr_t)0, memory_order_release);
}

/* ============== Insert Operations ============== */

int ll_insert_head(ll_head_t *list, void *elm)
{
    if (!list || !elm)
        return LL_ERR_INVAL;
    if (!tls_thread_state)
        return LL_ERR_NOTHREAD;

    /* Allocate wrapper node. */
    versioned_node_t *w = (versioned_node_t *)aligned_alloc(
        alignof(versioned_node_t), sizeof(versioned_node_t));
    if (!w)
        return LL_ERR_NOMEM;

    /* Get transaction ID AFTER allocation succeeds. */
    uint64_t txn_id = atomic_fetch_add_explicit(&list->commit_id, 1,
                                                 memory_order_acq_rel);

    w->user_elm = elm;
    w->insert_txn_id = txn_id;
    atomic_store_explicit(&w->removed_txn_id, (uint64_t)0, memory_order_release);

    /* CAS loop to insert at head. */
    uintptr_t old_head;
    do {
        old_head = atomic_load_explicit(&list->head, memory_order_acquire);
        atomic_store_explicit(&w->next, old_head, memory_order_release);
    } while (!atomic_compare_exchange_weak_explicit(
        &list->head, &old_head, (uintptr_t)w,
        memory_order_release, memory_order_acquire));

    return LL_OK;
}
/* ============== Remove Operations ============== */

int ll_remove(ll_head_t *list, void *elm)
{
    if (!list || !elm)
        return LL_ERR_INVAL;
    if (!tls_thread_state)
        return LL_ERR_NOTHREAD;

    ll_thread_state_t *state = tls_thread_state;

    /* Get transaction ID for the remove. */
    uint64_t txn_id = atomic_fetch_add_explicit(&list->commit_id, 1,
                                                 memory_order_acq_rel);

    /* Traverse with hazard pointer protection. */
    versioned_node_t *curr = ptr_unmask(
        atomic_load_explicit(&list->head, memory_order_acquire));

    while (curr) {
        hp_acquire(state, 0, curr);

        /* Re-validate pointer. */
        versioned_node_t *check = ptr_unmask(
            atomic_load_explicit(&list->head, memory_order_acquire));

        /* Find curr in the list (it may have moved). */
        bool found_curr = false;
        versioned_node_t *scan = check;
        while (scan) {
            if (scan == curr) {
                found_curr = true;
                break;
            }
            scan = ptr_unmask(atomic_load_explicit(&scan->next, memory_order_acquire));
        }

        if (!found_curr) {
            /* Node was removed, restart. */
            hp_release(state, 0);
            curr = ptr_unmask(atomic_load_explicit(&list->head, memory_order_acquire));
            continue;
        }

        if (curr->user_elm == elm) {
            atomic_store_explicit(&curr->removed_txn_id, txn_id, memory_order_release);
            hp_release(state, 0);
            return LL_OK;
        }

        versioned_node_t *next = ptr_unmask(
            atomic_load_explicit(&curr->next, memory_order_acquire));
        hp_release(state, 0);
        curr = next;
    }

    return LL_ERR_NOTFOUND;
}

int ll_remove_first(ll_head_t *list, void **out_elm)
{
    if (!list || !out_elm)
        return LL_ERR_INVAL;
    if (!tls_thread_state)
        return LL_ERR_NOTHREAD;

    ll_thread_state_t *state = tls_thread_state;
    uint64_t snapshot = atomic_load_explicit(&list->commit_id, memory_order_acquire);

    for (;;) {
        uintptr_t head_val = atomic_load_explicit(&list->head, memory_order_acquire);
        versioned_node_t *w = ptr_unmask(head_val);
        if (!w)
            return LL_ERR_NOTFOUND;

        hp_acquire(state, 0, w);

        /* Verify head hasn't changed. */
        if (atomic_load_explicit(&list->head, memory_order_acquire) != head_val) {
            hp_release(state, 0);
            continue;
        }

        if (node_visible(w, snapshot)) {
            uintptr_t next_val = atomic_load_explicit(&w->next, memory_order_acquire);
            if (atomic_compare_exchange_weak_explicit(
                    &list->head, &head_val, next_val,
                    memory_order_release, memory_order_acquire)) {
                *out_elm = w->user_elm;
                hp_release(state, 0);
                free(w);
                return LL_OK;
            }
            hp_release(state, 0);
            continue;
        }

        /* Head not visible, search for first visible node. */
        versioned_node_t *prev = w;
        versioned_node_t *curr = ptr_unmask(
            atomic_load_explicit(&w->next, memory_order_acquire));
        bool cas_failed = false;

        while (curr) {
            hp_acquire(state, 1, curr);

            if (node_visible(curr, snapshot)) {
                uintptr_t next_val = atomic_load_explicit(&curr->next, memory_order_acquire);
                uintptr_t prev_next = (uintptr_t)curr;

                if (atomic_compare_exchange_weak_explicit(
                        &prev->next, &prev_next, next_val,
                        memory_order_release, memory_order_acquire)) {
                    *out_elm = curr->user_elm;
                    hp_release_all(state);
                    free(curr);
                    return LL_OK;
                }
                cas_failed = true;
                break;
            }

            prev = curr;
            curr = ptr_unmask(atomic_load_explicit(&curr->next, memory_order_acquire));
        }

        hp_release_all(state);
        if (cas_failed)
            continue;
        return LL_ERR_NOTFOUND;
    }
}

/* ============== Iterator & Traversal ============== */

int ll_iterator_begin(ll_head_t *list, ll_iterator_t *iter)
{
    if (!list || !iter)
        return LL_ERR_INVAL;
    if (!tls_thread_state)
        return LL_ERR_NOTHREAD;

    iter->list = list;
    iter->snapshot = atomic_load_explicit(&list->commit_id, memory_order_acquire);
    iter->current_node = NULL;

    /* Register active snapshot. */
    atomic_store_explicit(&tls_thread_state->active_snapshot, iter->snapshot,
                          memory_order_release);

    return LL_OK;
}

void *ll_iterator_next(ll_iterator_t *iter)
{
    if (!iter || !iter->list || !tls_thread_state)
        return NULL;

    ll_thread_state_t *state = tls_thread_state;
    versioned_node_t *curr;

    if (iter->current_node == NULL) {
        /* First call - start from head. */
        curr = ptr_unmask(atomic_load_explicit(&iter->list->head, memory_order_acquire));
    } else {
        /* Continue from after current node. */
        curr = ptr_unmask(atomic_load_explicit(
            &((versioned_node_t *)iter->current_node)->next, memory_order_acquire));
    }

    /* Find next visible node. */
    while (curr) {
        hp_acquire(state, 0, curr);

        if (node_visible(curr, iter->snapshot)) {
            iter->current_node = curr;
            hp_release(state, 0);
            return curr->user_elm;
        }

        versioned_node_t *next = ptr_unmask(
            atomic_load_explicit(&curr->next, memory_order_acquire));
        hp_release(state, 0);
        curr = next;
    }

    iter->current_node = NULL;
    return NULL;
}

void ll_iterator_end(ll_iterator_t *iter)
{
    if (!iter)
        return;

    if (tls_thread_state) {
        atomic_store_explicit(&tls_thread_state->active_snapshot, (uint64_t)0,
                              memory_order_release);
    }

    iter->list = NULL;
    iter->current_node = NULL;
    iter->snapshot = 0;
}

uint64_t ll_iterator_snapshot(const ll_iterator_t *iter)
{
    return iter ? iter->snapshot : 0;
}

/* ============== Utility Functions ============== */

bool ll_is_empty(ll_head_t *list)
{
    if (!list)
        return true;

    uint64_t snapshot = atomic_load_explicit(&list->commit_id, memory_order_acquire);
    versioned_node_t *curr = ptr_unmask(
        atomic_load_explicit(&list->head, memory_order_acquire));

    while (curr) {
        if (node_visible(curr, snapshot))
            return false;
        curr = ptr_unmask(atomic_load_explicit(&curr->next, memory_order_acquire));
    }
    return true;
}

bool ll_contains(ll_head_t *list, const void *elm)
{
    if (!list || !elm)
        return false;

    uint64_t snapshot = atomic_load_explicit(&list->commit_id, memory_order_acquire);
    versioned_node_t *curr = ptr_unmask(
        atomic_load_explicit(&list->head, memory_order_acquire));

    while (curr) {
        if (node_visible(curr, snapshot) && curr->user_elm == elm)
            return true;
        curr = ptr_unmask(atomic_load_explicit(&curr->next, memory_order_acquire));
    }
    return false;
}

size_t ll_count(ll_head_t *list)
{
    if (!list)
        return 0;

    size_t count = 0;
    uint64_t snapshot = atomic_load_explicit(&list->commit_id, memory_order_acquire);
    versioned_node_t *curr = ptr_unmask(
        atomic_load_explicit(&list->head, memory_order_acquire));

    while (curr) {
        if (node_visible(curr, snapshot))
            count++;
        curr = ptr_unmask(atomic_load_explicit(&curr->next, memory_order_acquire));
    }
    return count;
}

/* ============== Memory Reclamation ============== */

void ll_reclaim(ll_head_t *list, void (*free_cb)(void *))
{
    if (!list || !list->domain || !tls_thread_state)
        return;

    ll_domain_t *domain = list->domain;
    ll_thread_state_t *state = tls_thread_state;

    uint64_t min_snap = min_active_snapshot(domain);
    if (min_snap == UINT64_MAX)
        min_snap = atomic_load_explicit(&list->commit_id, memory_order_acquire);

    versioned_node_t *prev = NULL;
    versioned_node_t *curr = ptr_unmask(
        atomic_load_explicit(&list->head, memory_order_acquire));

    while (curr) {
        uint64_t rid = atomic_load_explicit(&curr->removed_txn_id, memory_order_acquire);
        bool reclaimable = (rid != 0 && rid < min_snap);

        uintptr_t next_val = atomic_load_explicit(&curr->next, memory_order_acquire);
        versioned_node_t *next = ptr_unmask(next_val);

        if (reclaimable) {
            hp_acquire(state, 0, curr);
            uintptr_t unlink_val = (uintptr_t)curr;
            bool unlinked = false;

            if (prev) {
                if (atomic_compare_exchange_weak_explicit(
                        &prev->next, &unlink_val, next_val,
                        memory_order_release, memory_order_acquire))
                    unlinked = true;
            } else {
                if (atomic_compare_exchange_weak_explicit(
                        &list->head, &unlink_val, next_val,
                        memory_order_release, memory_order_acquire))
                    unlinked = true;
            }

            if (unlinked) {
                hp_release(state, 0);
                /* Add to retired list. */
                atomic_store_explicit(&curr->next, (uintptr_t)state->retired_list,
                                      memory_order_relaxed);
                state->retired_list = curr;
                curr = next;
                continue;
            }
            hp_release(state, 0);
        }

        prev = curr;
        curr = next;
    }

    /* Try to free retired nodes. */
    versioned_node_t *still_held = NULL;
    while (state->retired_list) {
        versioned_node_t *n = state->retired_list;
        state->retired_list = ptr_unmask(
            atomic_load_explicit(&n->next, memory_order_relaxed));

        if (any_hp_equals(domain, n)) {
            atomic_store_explicit(&n->next, (uintptr_t)still_held, memory_order_relaxed);
            still_held = n;
        } else {
            void *user = n->user_elm;
            free(n);
            if (free_cb)
                free_cb(user);
        }
    }
    state->retired_list = still_held;
}

/* ============== Legacy API (Deprecated) ============== */

/*
 * Global default domain for legacy API.
 * Lazily initialized on first use.
 */
static ll_domain_t *legacy_domain = NULL;
static atomic_flag legacy_init_lock = ATOMIC_FLAG_INIT;

static ll_domain_t *get_legacy_domain(void)
{
    if (legacy_domain)
        return legacy_domain;

    while (atomic_flag_test_and_set(&legacy_init_lock)) {
        /* Spin. */
    }

    if (!legacy_domain) {
        legacy_domain = ll_domain_create(32);
    }

    atomic_flag_clear(&legacy_init_lock);
    return legacy_domain;
}

static void ensure_legacy_thread_registered(void)
{
    ll_domain_t *domain = get_legacy_domain();
    if (domain && tls_domain != domain) {
        ll_thread_register(domain);
    }
}

void ll_init_(atomic_uintptr_t *head, ll_commit_id_t *commit_id)
{
    atomic_store_explicit(head, (uintptr_t)0, memory_order_release);
    atomic_store_explicit(commit_id, 1, memory_order_release);
}

void ll_insert_head_(atomic_uintptr_t *head, ll_commit_id_t *commit_id, void *elm)
{
    ensure_legacy_thread_registered();

    /* Allocate wrapper node first (don't increment commit_id on failure). */
    versioned_node_t *w = (versioned_node_t *)aligned_alloc(
        alignof(versioned_node_t), sizeof(versioned_node_t));
    if (!w)
        return;

    uint64_t txn_id = atomic_fetch_add_explicit(commit_id, 1, memory_order_acq_rel);

    w->user_elm = elm;
    w->insert_txn_id = txn_id;
    atomic_store_explicit(&w->removed_txn_id, (uint64_t)0, memory_order_release);

    uintptr_t old_head;
    do {
        old_head = atomic_load_explicit(head, memory_order_acquire);
        atomic_store_explicit(&w->next, old_head, memory_order_release);
    } while (!atomic_compare_exchange_weak_explicit(
        head, &old_head, (uintptr_t)w,
        memory_order_release, memory_order_acquire));
}

void *ll_remove_head_(atomic_uintptr_t *head, ll_commit_id_t *commit_id)
{
    ensure_legacy_thread_registered();

    if (!tls_thread_state)
        return NULL;

    ll_thread_state_t *state = tls_thread_state;
    uint64_t snapshot = atomic_load_explicit(commit_id, memory_order_acquire);

    for (;;) {
        uintptr_t head_val = atomic_load_explicit(head, memory_order_acquire);
        versioned_node_t *w = ptr_unmask(head_val);
        if (!w)
            return NULL;

        hp_acquire(state, 0, w);

        if (atomic_load_explicit(head, memory_order_acquire) != head_val) {
            hp_release(state, 0);
            continue;
        }

        if (node_visible(w, snapshot)) {
            uintptr_t next_val = atomic_load_explicit(&w->next, memory_order_acquire);
            if (atomic_compare_exchange_weak_explicit(
                    head, &head_val, next_val,
                    memory_order_release, memory_order_acquire)) {
                void *user = w->user_elm;
                hp_release(state, 0);
                free(w);
                return user;
            }
            hp_release(state, 0);
            continue;
        }

        /* Head not visible, find first visible. */
        versioned_node_t *prev = w;
        versioned_node_t *curr = ptr_unmask(
            atomic_load_explicit(&w->next, memory_order_acquire));
        bool cas_failed = false;

        while (curr) {
            hp_acquire(state, 1, curr);

            if (node_visible(curr, snapshot)) {
                uintptr_t next_val = atomic_load_explicit(&curr->next, memory_order_acquire);
                uintptr_t prev_next = (uintptr_t)curr;

                if (atomic_compare_exchange_weak_explicit(
                        &prev->next, &prev_next, next_val,
                        memory_order_release, memory_order_acquire)) {
                    void *user = curr->user_elm;
                    hp_release_all(state);
                    free(curr);
                    return user;
                }
                cas_failed = true;
                break;
            }

            prev = curr;
            curr = ptr_unmask(atomic_load_explicit(&curr->next, memory_order_acquire));
        }

        hp_release_all(state);
        if (cas_failed)
            continue;
        return NULL;
    }
}

int ll_remove_(atomic_uintptr_t *head, ll_commit_id_t *commit_id,
               void (*free_cb)(void *), void *elm)
{
    (void)free_cb;  /* Unused in legacy API - kept for compatibility. */
    ensure_legacy_thread_registered();

    if (!tls_thread_state)
        return LL_ERR_NOTHREAD;

    ll_thread_state_t *state = tls_thread_state;

    uint64_t txn_id = atomic_fetch_add_explicit(commit_id, 1, memory_order_acq_rel);

    versioned_node_t *curr = ptr_unmask(
        atomic_load_explicit(head, memory_order_acquire));

    while (curr) {
        hp_acquire(state, 0, curr);

        /* Re-check that curr is still in list. */
        bool valid = false;
        versioned_node_t *scan = ptr_unmask(
            atomic_load_explicit(head, memory_order_acquire));
        while (scan) {
            if (scan == curr) {
                valid = true;
                break;
            }
            scan = ptr_unmask(atomic_load_explicit(&scan->next, memory_order_acquire));
        }

        if (!valid) {
            hp_release(state, 0);
            curr = ptr_unmask(atomic_load_explicit(head, memory_order_acquire));
            continue;
        }

        if (curr->user_elm == elm) {
            atomic_store_explicit(&curr->removed_txn_id, txn_id, memory_order_release);
            hp_release(state, 0);
            return 0;
        }

        versioned_node_t *next = ptr_unmask(
            atomic_load_explicit(&curr->next, memory_order_acquire));
        hp_release(state, 0);
        curr = next;
    }

    return -1;
}

uint64_t ll_snapshot_begin(ll_commit_id_t *commit_id)
{
    ensure_legacy_thread_registered();

    uint64_t snapshot = atomic_load_explicit(commit_id, memory_order_acquire);

    if (tls_thread_state) {
        atomic_store_explicit(&tls_thread_state->active_snapshot, snapshot,
                              memory_order_release);
    }

    return snapshot;
}

void ll_snapshot_end(void)
{
    if (tls_thread_state) {
        atomic_store_explicit(&tls_thread_state->active_snapshot, (uint64_t)0,
                              memory_order_release);
    }
}

void *ll_snapshot_first_(atomic_uintptr_t *head, ll_commit_id_t *commit_id,
                         uint64_t snapshot_version)
{
    ensure_legacy_thread_registered();

    if (snapshot_version == 0)
        snapshot_version = atomic_load_explicit(commit_id, memory_order_acquire);

    ll_thread_state_t *state = tls_thread_state;
    versioned_node_t *curr = ptr_unmask(
        atomic_load_explicit(head, memory_order_acquire));

    while (curr) {
        if (state)
            hp_acquire(state, 0, curr);

        if (node_visible(curr, snapshot_version)) {
            if (state)
                hp_release(state, 0);
            return curr->user_elm;
        }

        versioned_node_t *next = ptr_unmask(
            atomic_load_explicit(&curr->next, memory_order_acquire));
        if (state)
            hp_release(state, 0);
        curr = next;
    }
    return NULL;
}

void *ll_snapshot_next_(atomic_uintptr_t *head, ll_commit_id_t *commit_id,
                        uint64_t snapshot_version, const void *elm)
{
    ensure_legacy_thread_registered();

    if (snapshot_version == 0)
        snapshot_version = atomic_load_explicit(commit_id, memory_order_acquire);

    ll_thread_state_t *state = tls_thread_state;
    versioned_node_t *curr = ptr_unmask(
        atomic_load_explicit(head, memory_order_acquire));

    /* Find the node containing elm. */
    while (curr) {
        if (state)
            hp_acquire(state, 0, curr);

        if (curr->user_elm == elm) {
            /* Found it, now find next visible. */
            curr = ptr_unmask(atomic_load_explicit(&curr->next, memory_order_acquire));
            if (state)
                hp_release(state, 0);

            while (curr) {
                if (state)
                    hp_acquire(state, 0, curr);

                if (node_visible(curr, snapshot_version)) {
                    if (state)
                        hp_release(state, 0);
                    return curr->user_elm;
                }

                versioned_node_t *next = ptr_unmask(
                    atomic_load_explicit(&curr->next, memory_order_acquire));
                if (state)
                    hp_release(state, 0);
                curr = next;
            }
            return NULL;
        }

        versioned_node_t *next = ptr_unmask(
            atomic_load_explicit(&curr->next, memory_order_acquire));
        if (state)
            hp_release(state, 0);
        curr = next;
    }
    return NULL;
}

void ll_reclaim_(atomic_uintptr_t *head, ll_commit_id_t *commit_id,
                 void (*free_cb)(void *))
{
    ensure_legacy_thread_registered();

    if (!tls_thread_state)
        return;

    ll_domain_t *domain = get_legacy_domain();
    ll_thread_state_t *state = tls_thread_state;

    uint64_t min_snap = min_active_snapshot(domain);
    if (min_snap == UINT64_MAX)
        min_snap = atomic_load_explicit(commit_id, memory_order_acquire);

    versioned_node_t *prev = NULL;
    versioned_node_t *curr = ptr_unmask(
        atomic_load_explicit(head, memory_order_acquire));

    while (curr) {
        uint64_t rid = atomic_load_explicit(&curr->removed_txn_id, memory_order_acquire);
        bool reclaimable = (rid != 0 && rid < min_snap);

        uintptr_t next_val = atomic_load_explicit(&curr->next, memory_order_acquire);
        versioned_node_t *next = ptr_unmask(next_val);

        if (reclaimable) {
            hp_acquire(state, 0, curr);
            uintptr_t unlink_val = (uintptr_t)curr;
            bool unlinked = false;

            if (prev) {
                if (atomic_compare_exchange_weak_explicit(
                        &prev->next, &unlink_val, next_val,
                        memory_order_release, memory_order_acquire))
                    unlinked = true;
            } else {
                if (atomic_compare_exchange_weak_explicit(
                        head, &unlink_val, next_val,
                        memory_order_release, memory_order_acquire))
                    unlinked = true;
            }

            if (unlinked) {
                hp_release(state, 0);
                atomic_store_explicit(&curr->next, (uintptr_t)state->retired_list,
                                      memory_order_relaxed);
                state->retired_list = curr;
                curr = next;
                continue;
            }
            hp_release(state, 0);
        }

        prev = curr;
        curr = next;
    }

    /* Free retired nodes. */
    versioned_node_t *still_held = NULL;
    while (state->retired_list) {
        versioned_node_t *n = state->retired_list;
        state->retired_list = ptr_unmask(
            atomic_load_explicit(&n->next, memory_order_relaxed));

        if (any_hp_equals(domain, n)) {
            atomic_store_explicit(&n->next, (uintptr_t)still_held, memory_order_relaxed);
            still_held = n;
        } else {
            void *user = n->user_elm;
            free(n);
            if (free_cb)
                free_cb(user);
        }
    }
    state->retired_list = still_held;
}
