// object.c — Content-addressable object store
//
// Every piece of data (file contents, directory listings, commits) is stored
// as an "object" named by its SHA-256 hash. Objects are stored under
// .pes/objects/XX/YYYYYY... where XX is the first two hex characters of the
// hash (directory sharding).
//
// PROVIDED functions: compute_hash, object_path, object_exists, hash_to_hex, hex_to_hash
// TODO functions:     object_write, object_read

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
}

// Get the filesystem path where an object should be stored.
// Format: .pes/objects/XX/YYYYYYYY...
// The first 2 hex chars form the shard directory; the rest is the filename.
void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── TODO: Implement these ──────────────────────────────────────────────────

// Write an object to the store.
//
// Object format on disk:
//   "<type> <size>\0<data>"
//   where <type> is "blob", "tree", or "commit"
//   and <size> is the decimal string of the data length
//
//
// Returns 0 on success, -1 on error.
// object.c - Implementations for object_write and object_read

int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    // 1. Prepare header: "type size\0"
    const char *type_str = (type == OBJ_BLOB) ? "blob" : (type == OBJ_TREE ? "tree" : "commit");
    char header[64];
    int header_len = sprintf(header, "%s %zu", type_str, len) + 1;

    // 2. Combine header and data to compute hash
    size_t total_len = header_len + len;
    uint8_t *full_data = malloc(total_len);
    memcpy(full_data, header, header_len);
    memcpy(full_data + header_len, data, len);
    compute_hash(full_data, total_len, id_out);

    // 3. Determine path: .pes/objects/XX/YYYY...
        char path[512];
        object_path(id_out, path, sizeof(path));

        // Create directory (sharding)
        char dir[512];
        strncpy(dir, path, 15); // Extracts ".pes/objects/XX"
        dir[15] = '\0';
        mkdir(dir, 0755);

    // 4. Atomic Write: Write to .tmp then rename
    char tmp_path[520];
    sprintf(tmp_path, "%s.tmp", path);
    FILE *f = fopen(tmp_path, "wb");
    if (!f) { free(full_data); return -1; }

    fwrite(full_data, 1, total_len, f);
    fclose(f);
    rename(tmp_path, path);

    free(full_data);
    return 0;

}

int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    char path[512];
    object_path(id, path, sizeof(path));

    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    // Get file size
    fseek(f, 0, SEEK_END);
    size_t file_size = ftell(f);
    rewind(f);

    uint8_t *full_buf = malloc(file_size);
    fread(full_buf, 1, file_size, f);
    fclose(f);

    // 1. Integrity Check
    ObjectID actual_id;
    compute_hash(full_buf, file_size, &actual_id);
    if (memcmp(id->hash, actual_id.hash, HASH_SIZE) != 0) {
        free(full_buf);
        return -1;
    }

    // 2. Parse Header
    char *header = (char *)full_buf;
    char *data_start = memchr(header, '\0', file_size);
    if (!data_start) { free(full_buf); return -1; }
    data_start++; // Move past \0

}
