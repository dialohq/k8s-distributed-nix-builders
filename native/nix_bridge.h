#ifndef DISTRIBUTED_NIX_BRIDGE_H
#define DISTRIBUTED_NIX_BRIDGE_H
#include <stddef.h>
#include <stdint.h>
#ifdef __cplusplus
#define DISTRIBUTED_NIX_NOEXCEPT noexcept
extern "C" {
#else
#define DISTRIBUTED_NIX_NOEXCEPT
#endif

/* Versioned ABI: no C++ types or exceptions cross this boundary. */
#define DISTRIBUTED_NIX_DUMP 1u
#define DISTRIBUTED_NIX_CHECK 2u
#define DISTRIBUTED_NIX_REGISTER 3u
#define DISTRIBUTED_NIX_GC_SNAPSHOT 4u
#define DISTRIBUTED_NIX_GC_DELETE 5u
struct distributed_nix_buffer { unsigned char *data; size_t len; };

/* store: borrowed NUL-terminated UTF-8 store URI.
 * input: borrowed UTF-8 JSON bytes (root array for DUMP, manifest for CHECK/REGISTER, path array for GC_DELETE, null for GC_SNAPSHOT).
 * All borrowed memory must remain valid for this call only. result must point
 * to an empty buffer. On return it owns JSON (status 0), or error text (status 1).
 * Status 2 means invalid arguments or allocation failure; result may be empty.
 * Release either success or error buffers exactly once with buffer_free_v1.
 * Calls serialize access to Nix's process-global state and initialize it once.
 */
int distributed_nix_call_v1(uint32_t operation, const char *store,
    const unsigned char *input, size_t input_len,
    struct distributed_nix_buffer *result) DISTRIBUTED_NIX_NOEXCEPT;
void distributed_nix_buffer_free_v1(struct distributed_nix_buffer *buffer) DISTRIBUTED_NIX_NOEXCEPT;
/* A dedicated, single-client process serves Nix's native worker protocol on
 * stdin/stdout. callback buffers use malloc/free, just like the bridge buffers.
 * callback op 1 durably enqueues registration; op 5 acquires online GC roots. */
int distributed_nix_serve_v1(int trusted, struct distributed_nix_buffer *result) DISTRIBUTED_NIX_NOEXCEPT;
int distributed_nix_serve_transfer_v2(const char *store, int writable, struct distributed_nix_buffer *result) DISTRIBUTED_NIX_NOEXCEPT;
int distributed_nix_runtime_v1(uint32_t operation, const unsigned char *input,
    size_t input_len, struct distributed_nix_buffer *result);
#ifdef __cplusplus
}
#endif
#undef DISTRIBUTED_NIX_NOEXCEPT
#endif
