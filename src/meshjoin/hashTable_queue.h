#ifndef HASHTABLE_QUEUE_H
#define HASHTABLE_QUEUE_H

/* ============================================================================
 * hashTable_queue.h
 * ----------------------------------------------------------------------------
 * Pure-C, header-only, vanilla implementations of:
 *   1) hash_table  : generic multimap (works with any key and any value struct)
 *   2) queue       : doubly linked list FIFO
 *
 * Used by: src/meshjoin/stream_buffer.h, disk_buffer.h, master_data.cpp
 *
 * Design intent (per DWH Project PDF, Section 4.1):
 *   - H is a MULTI-MAP keyed by the join attribute (customer_id / product_id).
 *   - Many transactions may share one key.
 *   - Values are copied into H. The queue does NOT keep a second copy; it only
 *     stores the handles returned by ht_insert() (a chunk = array of handles).
 *   - The queue is FIFO: oldest chunk dequeued first after it has been compared
 *     against the whole of the Master Data.
 *
 * Thread-safety: NONE. Caller must serialize access (use pthread mutex if needed).
 * Include this header in exactly ONE translation unit per module to avoid
 * duplicate-static-bloat, or keep it static-inline as-is (both are safe).
 * ==========================================================================*/


#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

/* ============================================================================
 *  PART 1  —  GENERIC HASH TABLE (MULTIMAP)
 * ==========================================================================*/

typedef struct ht_entry      ht_entry;
typedef struct ht_value_node ht_value_node;

struct ht_value_node {
    void          *value;        /* copy of the caller's value blob          */
    size_t         value_size;
    ht_entry      *owner;        /* back-pointer for O(1) removal            */
    ht_value_node *next;         /* next value sharing the same key          */
};

struct ht_entry {
    void          *key;          /* copy of the caller's key blob            */
    size_t         key_size;
    ht_value_node *values;       /* linked list of values for this key       */
    size_t         value_count;
    ht_entry      *next;         /* bucket chain                             */
};

typedef struct {
    ht_entry **buckets;
    size_t     bucket_count;
    size_t     key_size;
    size_t     value_size;
    size_t     num_keys;         /* distinct key entries                     */
    size_t     num_values;       /* total inserted values (transactions)     */
    uint64_t (*hash_fn)(const void *key, size_t key_size);
    int      (*eq_fn)  (const void *a,   const void *b, size_t key_size);
} hash_table;

typedef struct {
    ht_value_node *current;
    int            valid;
} ht_iter;

/* ---- default hash (FNV-1a) and equality (memcmp) -------------------------*/
static inline uint64_t ht_default_hash(const void *key, size_t key_size)
{
    const unsigned char *p = (const unsigned char *)key;
    uint64_t h = 1469598103934665603ULL;          /* offset basis */
    for (size_t i = 0; i < key_size; ++i) {
        h ^= (uint64_t)p[i];
        h *= 1099511628211ULL;                     /* FNV prime    */
    }
    return h;
}

static inline int ht_default_eq(const void *a, const void *b, size_t key_size)
{
    return memcmp(a, b, key_size) == 0;
}

/* ---- constructor / destructor -------------------------------------------*/
static inline hash_table *ht_create(size_t key_size,
                                    size_t value_size,
                                    size_t bucket_count,
                                    uint64_t (*hash_fn)(const void *, size_t),
                                    int      (*eq_fn)  (const void *, const void *, size_t))
{
    if (key_size == 0 || value_size == 0) return NULL;
    if (bucket_count == 0) bucket_count = 101;     /* small prime default */

    hash_table *ht = (hash_table *)calloc(1, sizeof(*ht));
    if (!ht) return NULL;

    ht->buckets      = (ht_entry **)calloc(bucket_count, sizeof(ht_entry *));
    if (!ht->buckets) { free(ht); return NULL; }

    ht->bucket_count = bucket_count;
    ht->key_size     = key_size;
    ht->value_size   = value_size;
    ht->hash_fn      = hash_fn ? hash_fn : ht_default_hash;
    ht->eq_fn        = eq_fn   ? eq_fn   : ht_default_eq;
    return ht;
}

static inline void ht_clear(hash_table *ht)
{
    if (!ht) return;
    for (size_t i = 0; i < ht->bucket_count; ++i) {
        ht_entry *e = ht->buckets[i];
        while (e) {
            ht_entry *enext = e->next;
            ht_value_node *v = e->values;
            while (v) {
                ht_value_node *vnext = v->next;
                free(v->value);
                free(v);
                v = vnext;
            }
            free(e->key);
            free(e);
            e = enext;
        }
        ht->buckets[i] = NULL;
    }
    ht->num_keys   = 0;
    ht->num_values = 0;
}

static inline void ht_destroy(hash_table *ht)
{
    if (!ht) return;
    ht_clear(ht);
    free(ht->buckets);
    free(ht);
}

/* ---- insert (multimap: appends, does not overwrite) ---------------------*/
/* Returns an opaque handle to the inserted value node. The caller
 * (e.g. the MESHJOIN chunk) keeps these handles and later passes them back
 * to ht_remove_node() so the queue never needs a second copy of the data. */
static inline ht_value_node *ht_insert(hash_table *ht,
                                       const void *key,
                                       const void *value)
{
    if (!ht || !key || !value) return NULL;

    size_t idx = (size_t)(ht->hash_fn(key, ht->key_size) % ht->bucket_count);

    /* Find or create the key entry. */
    ht_entry *entry = ht->buckets[idx];
    while (entry && !ht->eq_fn(entry->key, key, ht->key_size))
        entry = entry->next;

    if (!entry) {
        entry = (ht_entry *)calloc(1, sizeof(*entry));
        if (!entry) return NULL;
        entry->key = malloc(ht->key_size);
        if (!entry->key) { free(entry); return NULL; }
        memcpy(entry->key, key, ht->key_size);
        entry->key_size = ht->key_size;
        entry->next     = ht->buckets[idx];
        ht->buckets[idx] = entry;
        ht->num_keys++;
    }

    /* Allocate the value node (the multimap value). */
    ht_value_node *vn = (ht_value_node *)calloc(1, sizeof(*vn));
    if (!vn) return NULL;
    vn->value = malloc(ht->value_size);
    if (!vn->value) { free(vn); return NULL; }
    memcpy(vn->value, value, ht->value_size);
    vn->value_size = ht->value_size;
    vn->owner      = entry;

    /* Prepend to the value list (O(1)). */
    vn->next       = entry->values;
    entry->values  = vn;
    entry->value_count++;
    ht->num_values++;
    return vn;
}

/* ---- remove one value node (used when a chunk leaves memory) ------------*/
/* Frees the value blob and node; unlinks the key entry if it becomes empty. */
static inline void ht_remove_node(hash_table *ht, ht_value_node *vn)
{
    if (!ht || !vn) return;
    ht_entry *entry = vn->owner;

    /* Unlink vn from entry->values. */
    ht_value_node **pp = &entry->values;
    while (*pp && *pp != vn) pp = &(*pp)->next;
    if (*pp == vn) {
        *pp = vn->next;
        entry->value_count--;
        ht->num_values--;
        free(vn->value);
        free(vn);
    }

    /* If the key entry is now empty, unlink it from the bucket. */
    if (entry->value_count == 0) {
        size_t idx = (size_t)(ht->hash_fn(entry->key, entry->key_size)
                              % ht->bucket_count);
        ht_entry **pp2 = &ht->buckets[idx];
        while (*pp2 && *pp2 != entry) pp2 = &(*pp2)->next;
        if (*pp2 == entry) *pp2 = entry->next;
        free(entry->key);
        free(entry);
        ht->num_keys--;
    }
}

/* ---- lookup : returns an iterator over ALL values sharing one key -------*/
static inline ht_iter ht_find(const hash_table *ht, const void *key)
{
    ht_iter it = { NULL, 0 };
    if (!ht || !key) return it;

    size_t idx = (size_t)(ht->hash_fn(key, ht->key_size) % ht->bucket_count);
    for (ht_entry *e = ht->buckets[idx]; e; e = e->next) {
        if (ht->eq_fn(e->key, key, ht->key_size)) {
            it.current = e->values;
            it.valid   = (it.current != NULL);
            return it;
        }
    }
    return it;
}

/* Advance the iterator. Returns the next value pointer, or NULL when done. */
static inline void *ht_iter_next(ht_iter *it)
{
    if (!it || !it->valid || !it->current) { if (it) it->valid = 0; return NULL; }
    void *v = it->current->value;
    it->current = it->current->next;
    if (!it->current) it->valid = 0;
    return v;
}

/* ---- misc accessors ----------------------------------------------------*/
static inline size_t ht_num_keys  (const hash_table *ht) { return ht ? ht->num_keys   : 0; }
static inline size_t ht_num_values(const hash_table *ht) { return ht ? ht->num_values : 0; }

/* ---- full-scan walker (useful for debugging / status command) ----------*/
/* cb returns 0 to stop the walk, nonzero to continue. */
static inline void ht_foreach(const hash_table *ht,
                              int (*cb)(const void *key, void *value, void *user),
                              void *user)
{
    if (!ht || !cb) return;
    for (size_t i = 0; i < ht->bucket_count; ++i) {
        for (ht_entry *e = ht->buckets[i]; e; e = e->next) {
            for (ht_value_node *v = e->values; v; v = v->next) {
                if (!cb(e->key, v->value, user)) return;
            }
        }
    }
}

/* ============================================================================
 *  PART 2  —  DOUBLY LINKED LIST QUEUE
 * ===========================================================================*/

typedef struct queue_node {
    void              *data;     /* caller-owned pointer (chunk pointer)     */
    struct queue_node *prev;
    struct queue_node *next;
} queue_node;

typedef struct {
    queue_node *front;           /* oldest element (FIFO dequeue point)      */
    queue_node *back;            /* newest element  (FIFO enqueue point)     */
    size_t      size;
} queue;

static inline queue *queue_create(void)
{
    return (queue *)calloc(1, sizeof(queue));
}

/* free_fn, if non-NULL, is called on each element's data pointer during
 * clear/destroy. Pass NULL if the data is owned elsewhere (e.g. by the
 * hash table — which is the case for MESHJOIN chunks). */
static inline void queue_clear(queue *q, void (*free_fn)(void *))
{
    if (!q) return;
    queue_node *n = q->front;
    while (n) {
        queue_node *next = n->next;
        if (free_fn && n->data) free_fn(n->data);
        free(n);
        n = next;
    }
    q->front = q->back = NULL;
    q->size  = 0;
}

static inline void queue_destroy(queue *q, void (*free_fn)(void *))
{
    if (!q) return;
    queue_clear(q, free_fn);
    free(q);
}

/* Enqueue at the back. Returns 1 on success, 0 on OOM. */
static inline int queue_enqueue(queue *q, void *data)
{
    if (!q) return 0;
    queue_node *n = (queue_node *)calloc(1, sizeof(*n));
    if (!n) return 0;
    n->data = data;
    n->prev = q->back;
    n->next = NULL;
    if (q->back) q->back->next = n;
    else         q->front      = n;
    q->back  = n;
    q->size++;
    return 1;
}

/* Dequeue from the front (oldest). Returns NULL if empty. */
static inline void *queue_dequeue(queue *q)
{
    if (!q || !q->front) return NULL;
    queue_node *n = q->front;
    void *d = n->data;
    q->front = n->next;
    if (q->front) q->front->prev = NULL;
    else          q->back        = NULL;
    free(n);
    q->size--;
    return d;
}

/* Remove from the back (used if the algorithm ever needs LIFO shedding). */
static inline void *queue_dequeue_back(queue *q)
{
    if (!q || !q->back) return NULL;
    queue_node *n = q->back;
    void *d = n->data;
    q->back = n->prev;
    if (q->back) q->back->next = NULL;
    else         q->front      = NULL;
    free(n);
    q->size--;
    return d;
}

static inline void *queue_peek_front(const queue *q) { return (q && q->front) ? q->front->data : NULL; }
static inline void *queue_peek_back (const queue *q) { return (q && q->back ) ? q->back ->data : NULL; }
static inline size_t queue_size     (const queue *q) { return q ? q->size : 0; }
static inline int    queue_is_empty (const queue *q) { return !q || q->size == 0; }

/* ============================================================================
 *  MINIMAL USAGE SKETCH  (for MESHJOIN — not compiled into your build)
 * ----------------------------------------------------------------------------
 *  // 1. Create the hash table H keyed by int64_t customer_id, values are
 *  //    your transaction struct.
 *      hash_table *H = ht_create(sizeof(int64_t),   // key size
 *                                sizeof(txn_t),     // value size
 *                                4096,              // buckets
 *                                NULL, NULL);       // default hash / eq
 *
 *  // 2. Chunk handle: array of ht_value_node* per MESHJOIN chunk of 500.
 *      typedef struct { ht_value_node *items[500]; size_t n; } chunk_t;
 *      queue *Q = queue_create();
 *
 *  // 3. For each incoming transaction:
 *      ht_value_node *h = ht_insert(H, &txn.customer_id, &txn);
 *      chunk->items[chunk->n++] = h;                 // remember handle
 *
 *  // 4. When a chunk is complete, push it:
 *      queue_enqueue(Q, chunk);
 *
 *  // 5. Master Data probe on customer_id:
 *      ht_iter it = ht_find(H, &md_customer_id);
 *      txn_t *t;
 *      while ((t = (txn_t*)ht_iter_next(&it)) != NULL) {
 *          // enrich t with this Master Data tuple
 *      }
 *
 *  // 6. When a chunk has been probed against ALL Master Data partitions,
 *  //    pop it and free the handles:
 *      chunk_t *done = (chunk_t*)queue_dequeue(Q);
 *      for (size_t i = 0; i < done->n; ++i) ht_remove_node(H, done->items[i]);
 * ==========================================================================*/

#endif /* HASHTABLE_QUEUE_H */