/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  Fluent Bit
 *  ==========
 *  Copyright (C) 2015-2022 The Fluent Bit Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include <fluent-bit/flb_info.h>
#include <fluent-bit/flb_filter.h>
#include <fluent-bit/flb_filter_plugin.h>
#include <fluent-bit/flb_csv.h>
#include <fluent-bit/flb_utils.h>
#include <fluent-bit/flb_sds.h>
#include <fluent-bit/flb_file.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <avro.h>
#include <jansson.h>
#include <sys/param.h>

#include "avro/basics.h"
#include "avro/errors.h"
#include "avro/io.h"
#include "avro/schema.h"
#include "avro/src/avro/errors.h"
#include "avro/src/avro/generic.h"
#include "avro/src/avro/io.h"
#include "avro/src/avro/value.h"
#include "avro/src/schema.h"
#include "avro/src/st.h"
#include "avro/value.h"
#include "filter_avro.h"
#include "fluent-bit/flb_config.h"
#include "fluent-bit/flb_mem.h"
#include "fluent-bit/flb_str.h"
#include "fluent-bit/flb_time.h"
#include "mpack/mpack.h"

struct avro_union_data {
    const char *field;
    size_t field_len;
    int discriminant;
    int type;
};

static void write_avro_field(void *data, const char *field, size_t field_len);

static const char logev_schema[] =
"{"
"  \"type\":\"record\","
"  \"name\":\"LogEvent\","
"  \"fields\":["
"    {\"name\":\"metadata\",\"type\":\"bytes\"},"
"    {\"name\":\"avro_schema\",\"type\":\"string\"},"
"    {\"name\":\"max_size\",\"type\":\"int\"},"
"    {\"name\":\"payload\",\"type\":{\"type\":\"array\",\"items\":\"bytes\"}}"
"  ]"
"}";

static const char meta_schema[] =
"{"
"  \"type\":\"record\","
"  \"name\":\"metadata\","
"  \"fields\":["
"    {\"name\":\"wd_platform\",\"type\":\"string\"},"
"    {\"name\":\"wd_env_physical\",\"type\":\"string\"},"
"    {\"name\":\"wd_dc_physical\",\"type\":\"string\"},"
"    {\"name\":\"wd_env_logical\",\"type\":\"string\"},"
"    {\"name\":\"wd_service\",\"type\":\"string\"},"
"    {\"name\":\"wd_owner\",\"type\":\"string\"},"
"    {\"name\":\"wd_datatype\",\"type\":\"string\"},"
"    {\"name\":\"wd_objectname\",\"type\":\"string\"},"
"    {\"name\":\"wd_solas\",\"type\":\"string\"},"
"    {\"name\":\"swh_server\",\"type\":\"string\"},"
"    {\"name\":\"wd_service_instance\",\"type\":\"string\"}"
"  ]"
"}";

/* these values come first from the fluent-bit record,
 * and if not present fall back to environment variables.
 * default value is an empty string */
struct record_metadata {
    char *wd_platform;
    char *wd_env_physical;
    char *wd_dc_physical;
    char *wd_env_logical;
    char *wd_service;
    char *wd_owner;
    char *wd_datatype;
    char *wd_objectname;
    char *wd_solas;
    char *swh_server;
    char *wd_service_instance;
};

static int create_avro_schemas(struct filter_avro *ctx)
{
    char hostname[256];
    avro_value_t value;
    size_t field_count;
    size_t i;

    if (avro_schema_from_json_literal(logev_schema, &ctx->logev_schema)) {
        return -1;
    }

    ctx->logev_class = avro_generic_class_from_schema(ctx->logev_schema);
    if (!ctx->logev_class) {
        return -1;
    }

    if (avro_schema_from_json_literal(meta_schema, &ctx->meta_schema)) {
        return -1;
    }

    ctx->meta_class = avro_generic_class_from_schema(ctx->meta_schema);
    if (!ctx->meta_class) {
        return -1;
    }

    if (avro_generic_value_new(ctx->meta_class, &ctx->meta_value)) {
        return -1;
    }

    /* set default values for all metadata fields */
    field_count = avro_schema_record_size(ctx->meta_schema);

    for (i = 0; i < field_count; i++) {
        if (!avro_value_get_by_index(&ctx->meta_value, i, &value, NULL)) {
            avro_value_set_string_len(&value, "", 1);
        }
    }

    if (!gethostname(hostname, sizeof(hostname))) {
        if (!avro_value_get_by_name(&ctx->meta_value, "swh_server", &value, NULL)) {
            avro_value_set_string_len(&value, hostname, strlen(hostname) + 1);
        }
    }

    return 0;
}

static int cb_avro_init(struct flb_filter_instance *f_ins,
                        struct flb_config *config,
                        void *data)
{
    (void) data;
    struct filter_avro *ctx;

    /* Create context */
    ctx = flb_malloc(sizeof *ctx);
    if (!ctx) {
        flb_error("[filter_avro] filter cannot be loaded");
        return -1;
    }

    memset(ctx, 0, sizeof(*ctx));

    if (flb_filter_config_map_set(f_ins, (void*)ctx)) {
        flb_errno();
        flb_plg_error(f_ins, "configuration error");
        flb_free(ctx);
        return -1;
    }

    /* Set context */
    flb_filter_set_context(f_ins, ctx);
    ctx->ins = f_ins;

    ctx->packbuf = flb_sds_create_size(1024);
    if (!ctx->packbuf) {
        flb_error("[filter_avro] failed to allocate packbuf");
        return -1;
    }

    if (create_avro_schemas(ctx)) {
        flb_plg_error(ctx->ins, "failed to allocate avro lovev/meta schemas");
        return -1;
    }

    ctx->avro_write_buffer_size = 1024;
    ctx->avro_write_buffer = flb_malloc(ctx->avro_write_buffer_size);
    if (!ctx->avro_write_buffer) {
        flb_plg_error(ctx->ins, "Unable to allocate avro write buffer");
        return -1;
    }

    ctx->awriter = avro_writer_memory(ctx->avro_write_buffer,
            ctx->avro_write_buffer_size);
    if (!ctx->awriter) {
        flb_plg_error(ctx->ins, "failed to allocate avro writer");
        return -1;
    }

    return 0;
}

static flb_sds_t read_schema(const char *csv_file)
{
    char fname[4096];
    char *ext;

    strncpy(fname, csv_file, sizeof(fname));

    ext = strrchr(fname, '.');
    if (!ext) {
        return NULL;
    }

    strncpy(ext, ".json", sizeof(fname) - (ext - fname));

    return flb_file_read(fname);
}

static int read_key(
        mpack_reader_t *reader,
        const char *key,
        flb_sds_t *out)
{
    bool key_found;
    size_t key_len;
    size_t i;
    size_t len;
    size_t key_count;
    mpack_tag_t mtag;

    mtag = mpack_read_tag(reader);
    if (mtag.type != mpack_type_map) {
        /* failed to parse */
        return FLB_FILTER_NOTOUCH;
    }

    key_len = strlen(key);
    key_count = mpack_tag_map_count(&mtag);

    for (i = 0; i < key_count; i++) {
        mtag = mpack_read_tag(reader);
        if (mtag.type != mpack_type_str) {
            return FLB_FILTER_NOTOUCH;
        }

        key_found = mpack_tag_bytes(&mtag) == key_len &&
            !memcmp(reader->data, key, key_len);
        reader->data += mpack_tag_bytes(&mtag);
        mtag = mpack_read_tag(reader);
        if (mtag.type != mpack_type_str) {
            return FLB_FILTER_NOTOUCH;
        }
        len = mpack_tag_bytes(&mtag);
        if (key_found) {
            flb_sds_cat_safe(out, reader->data, len);
        }
        reader->data += len;
    }

    return 0;
}


static flb_sds_t read_tag(
        const char *data,
        size_t bytes)

{
    int ret;
    struct flb_time t;
    mpack_reader_t reader;
    flb_sds_t tag = flb_sds_create("");

    if (!tag) {
        return NULL;
    }

    mpack_reader_init_data(&reader, data, bytes);

    while (bytes > 0) {
        ret = flb_time_pop_from_mpack(&t, &reader);
        if (ret) {
            goto err;
        }

        ret = read_key(&reader, "file_name", &tag);
        if (ret) {
            goto err;
        }

        if (!flb_sds_len(tag)) {
            goto err;
        }

        return tag;
    }

err:
    flb_sds_destroy(tag);
    return NULL;
}

static struct filter_avro_tag_state *get_tag_state(
        struct filter_avro *ctx,
        const char *data,
        size_t bytes) {
    char *avro_schema_json;
    flb_sds_t json_root_str;
    json_t *json_root;
    json_t *schema_root;
    json_error_t json_error;
    int i;
    flb_sds_t tag;

    tag = read_tag(data, bytes);
    if (!tag) {
        flb_plg_error(ctx->ins,
                "Cannot read tag from \"file_name\" key");
        return NULL;
    }

    for (i = 0; i < FILTER_AVRO_MAX_TAG_COUNT; i++) {
        struct filter_avro_tag_state *state = ctx->states + i;
        if (state->used && !strcmp(state->tag, tag)) {
            flb_sds_destroy(tag);
            return state;
        }
    }
    /* not found, initialize for first use */
    for (i = 0; i < FILTER_AVRO_MAX_TAG_COUNT; i++) {
        struct filter_avro_tag_state *state = ctx->states + i;
        if (!state->used) {
            state->ctx = ctx;
            /* read avro schema */
            json_root_str = read_schema(tag);

            if (!json_root_str) {
                flb_plg_error(ctx->ins,
                        "Cannot find schema file for \"%s\"", tag);
                goto err;
            }

            json_root = json_loads(json_root_str, JSON_DECODE_ANY,
                    &json_error);
            if (!json_root) {
                flb_plg_error(ctx->ins,
                        "Unable to parse json schema:%s:error:%s:\n",
                        state->avro_schema_json,
                        json_error.text);
                goto err;
            }

            flb_sds_destroy(json_root_str);
            schema_root = json_object_get(json_root, "avro_schema");
            if (!schema_root) {
                flb_plg_error(ctx->ins,
                        "Unable to find avro_schema key",
                        state->avro_schema_json);
                goto err;
            }

            avro_schema_json = json_dumps(schema_root, JSON_ENCODE_ANY);
            state->avro_schema_json = flb_sds_create(avro_schema_json);
            if (!avro_schema_json || !state->avro_schema_json) {
                flb_plg_error(ctx->ins,
                        "To serialize avro_schema key");
                goto err;
            }

            flb_free(avro_schema_json);
            json_decref(json_root);

            if (avro_schema_from_json_length(state->avro_schema_json,
                        flb_sds_len(state->avro_schema_json), &state->aschema)) {
                flb_plg_error(ctx->ins,
                        "Unable to parse aobject schema:%s:error:%s:\n",
                        state->avro_schema_json,
                        avro_strerror());
                goto err;
            }

            state->aclass = avro_generic_class_from_schema(state->aschema);
            if (!state->aclass) {
                flb_plg_error(
                        ctx->ins,
                        "Unable to instantiate class from schema:%s:\n",
                        avro_strerror());
                goto err;
            }

            if (avro_generic_value_new(state->aclass, &state->record)) {
                flb_plg_error(
                        ctx->ins,
                        "Unable to allocate new avro value:%s:\n", avro_strerror(),
                        avro_strerror());
                goto err;
            }

            state->used = true;
            state->tag = flb_strdup(tag);
            state->row_buffer = flb_sds_create("");
            flb_csv_init(&state->state, write_avro_field, state);
            flb_sds_destroy(tag);

            return state;
        }
    }

err:
    flb_sds_destroy(tag);
    return NULL;
}

static void mpack_buffer_flush(mpack_writer_t* writer, const char* buffer, size_t count)
{
    struct filter_avro *ctx = writer->context;
    flb_sds_cat_safe(&ctx->packbuf, buffer, count);
}

static int collect_lines(
        struct filter_avro *ctx,
        struct filter_avro_tag_state *state,
        const char *data,
        size_t bytes)
{
    int ret;
    struct flb_time t;
    mpack_reader_t reader;

    mpack_reader_init_data(&reader, data, bytes);

    while (bytes > 0) {
        const char *record_start = reader.data;
        size_t record_size = 0;

        ret = flb_time_pop_from_mpack(&t, &reader);
        if (ret) {
            return ret;
        }

        ret = read_key(&reader, "log", &state->row_buffer);
        if (ret) {
            return ret;
        }

        flb_sds_cat_safe(&state->row_buffer, "\n", 1);
        record_size = reader.data - record_start;
        bytes -= record_size;
    }

    return 0;
}

static int extract_union_type_it(int i, avro_schema_t schema, void *arg)
{
    struct avro_union_data *data = arg;
    int type = avro_typeof(schema);

    switch (type) {
        case AVRO_NULL:
            if (!data->field_len) {
                data->discriminant = i;
                data->type = type;
            }
            break;
        default:
            if (data->field_len &&
                    (data->discriminant == -1 || type == AVRO_STRING)) {
                /* in the case of a union with more than 2 types,
                 * give preference to string if present, else use the
                 * first one */
                data->discriminant = i;
                data->type = type;
            }
            break;
    }

    return ST_CONTINUE;
}

static int extract_union_type(avro_value_t *value,
        const char *field, size_t field_len)
{
    struct avro_union_data data = {
        .field = field,
        .field_len = field_len,
        .discriminant = -1,
        .type = -1
    };
    avro_schema_t schema = avro_value_get_schema(value);
    struct avro_union_schema_t *uschema = avro_schema_to_union(schema);
    st_foreach(uschema->branches, HASH_FUNCTION_CAST extract_union_type_it,
            (st_data_t)&data);
    avro_value_set_branch(value, data.discriminant, value);
    return data.type;
}

static int extract_avro_type(avro_value_t *value,
        const char *field, size_t field_len)
{
    int type = avro_value_get_type(value);
    switch (type) {
        case AVRO_STRING:
        case AVRO_BYTES:
        case AVRO_BOOLEAN:
        case AVRO_INT32:
        case AVRO_INT64:
        case AVRO_FLOAT:
        case AVRO_FIXED:
        case AVRO_DOUBLE:
        case AVRO_NULL:
            return type;
        case AVRO_UNION:
            return extract_union_type(value, field, field_len);
        default:
            flb_error("unsupported avro type");
            return -1;
    }
}

static int parse_int64(const char *in, int64_t *out)
{
    char *end;
    int64_t val;

    errno = 0;
    val = strtol(in, &end, 10);
    if (end == in || *end != 0 || errno)  {
        return -1;
    }

    *out = val;
    return 0;
}

static int parse_double(const char *in, double *out)
{
    char *end;
    double val;
    errno = 0;
    val = strtod(in, &end);
    if (end == in || *end != 0 || errno) {
        return -1;
    }
    *out = val;
    return 0;
}

static void write_avro_int_value(const char *field, size_t field_len,
        avro_value_t *avalue)
{
    char buf[256];
    int64_t val;

    memcpy(buf, field, field_len);
    buf[field_len] = 0;
    parse_int64(buf, &val);

    switch (avro_value_get_type(avalue)) {
        case AVRO_INT32:
            avro_value_set_int(avalue, val);
            break;
        case AVRO_INT64:
            avro_value_set_long(avalue, val);
            break;
    }
}

static void write_avro_float_value(const char *field, size_t field_len,
        avro_value_t *avalue)
{
    char buf[256];
    double val;

    memcpy(buf, field, field_len);
    buf[field_len] = 0;
    parse_double(buf, &val);

    switch (avro_value_get_type(avalue)) {
        case AVRO_FLOAT:
            avro_value_set_float(avalue, val);
            break;
        case AVRO_DOUBLE:
            avro_value_set_double(avalue, val);
            break;
    }
}

static void write_avro_field(void *data, const char *field, size_t field_len)
{
    int ret;
    bool boolean_val;
    avro_value_t avalue;
    struct filter_avro_tag_state *state = data;
    const char *field_name;
    bool debug;

    debug = flb_log_check_level(state->ctx->ins->log_level, FLB_LOG_TRACE);

    ret = avro_value_get_by_index(
            &state->record,
            state->record_field_index,
            &avalue,
            debug ? &field_name : NULL);
 
    if (debug) {
        char csvfieldbuf[256];
        size_t count = MIN(field_len, sizeof(csvfieldbuf) - 4);
        memcpy(csvfieldbuf, field, count);
        if (count == sizeof(csvfieldbuf) - 4) {
            csvfieldbuf[count] = '.';
            csvfieldbuf[count+1] = '.';
            csvfieldbuf[count+2] = '.';
            csvfieldbuf[count+3] = 0;
        } else {
            csvfieldbuf[count] = 0;
        }
        flb_plg_trace(state->ctx->ins, "csv field (%s): \"%s\"",
                field_name, csvfieldbuf);
    }

    switch (extract_avro_type(&avalue, field, field_len)) {
        case AVRO_STRING:
            avro_value_set_string_len(&avalue, field, field_len + 1);
            break;
        case AVRO_BOOLEAN:
            boolean_val = field_len == 4 && !strncmp(field, "true", 4);
            avro_value_set_boolean(&avalue, boolean_val);
            break;
        case AVRO_INT32:
        case AVRO_INT64:
            write_avro_int_value(field, field_len, &avalue);
            break;
        case AVRO_FLOAT:
        case AVRO_DOUBLE:
            write_avro_float_value(field, field_len, &avalue);
            break;
        case AVRO_NULL:
            avro_value_set_null(&avalue);
            break;
        default:
            flb_error("unsupported avro type");
            break;
    }
    state->record_field_index++;
}

static ssize_t serialize_avro_value(
        struct filter_avro *ctx,
        avro_value_t *value)
{
    int ret;

    while ((ret = avro_value_write(ctx->awriter, value)) == ENOSPC) {
        ctx->avro_write_buffer_size *= 2;
        ctx->avro_write_buffer = flb_realloc(ctx->avro_write_buffer,
                ctx->avro_write_buffer_size);
        if (!ctx->avro_write_buffer) {
            flb_plg_error(ctx->ins, "Unable to allocate avro write buffer");
            return -1;
        }
        avro_writer_memory_set_dest(ctx->awriter, ctx->avro_write_buffer,
                ctx->avro_write_buffer_size);
    }

    if (ret) {
        flb_plg_error(ctx->ins, "Failed to serialize avro: %s", avro_strerror());
        return -1;
    }

    return avro_writer_tell(ctx->awriter);
}

static int write_avro_value(
        struct filter_avro *ctx,
        struct filter_avro_tag_state *state,
        avro_value_t *payload_array)
{
    ssize_t payload_size;
    avro_value_t payload;

    payload_size = serialize_avro_value(ctx, &state->record);
    if (payload_size < 0) {
        return -1;
    }

    if (avro_value_append(payload_array, &payload, NULL)) {
        flb_plg_error(ctx->ins, "failed to append avro payload to array: %s", 
                avro_strerror());
        return -1;
    }

    if (avro_value_set_bytes(&payload, ctx->avro_write_buffer, payload_size)) {
        flb_plg_error(ctx->ins, "failed to write avro payload: %s", avro_strerror());
        return -1;
    }
    ctx->payloads_total_size += payload_size;

    /* reset avro writer pointer */
    avro_writer_memory_set_dest(ctx->awriter, ctx->avro_write_buffer,
            ctx->avro_write_buffer_size);

    return 0;
}

static int populate_record_metadata(struct record_metadata *metadata)
{
}

static int metadata_to_avro(
        struct filter_avro *ctx,
        struct filter_avro_tag_state *state,
        avro_value_t *metadata)
{
    size_t payload_size;
    avro_value_t value;

    payload_size = serialize_avro_value(ctx, &ctx->meta_value);
    if (payload_size < 0) {
        flb_plg_error(ctx->ins, "serialize metadata: %s", avro_strerror());
        return -1;
    }
    /* reset avro writer pointer */
    avro_writer_memory_set_dest(ctx->awriter, ctx->avro_write_buffer,
            ctx->avro_write_buffer_size);

    if (avro_value_set_bytes(metadata, ctx->avro_write_buffer, payload_size)) {
        flb_plg_error(ctx->ins, "failed to write serialized metadata: %s",
                avro_strerror());
        return -1;
    }

    return 0;
}

static int csv_to_avro(
        struct filter_avro *ctx,
        struct filter_avro_tag_state *state,
        avro_value_t *payload_array)
{
    int ret;
    char *bufptr;
    char *bufptrstart;
    size_t buflen;
    size_t buflenstart;
    size_t field_count;

    ctx->payloads_total_size = 0;
    bufptr = state->row_buffer;
    buflen = flb_sds_len(state->row_buffer);
    /* parse all csv records */
    while (buflen) {
        bufptrstart = bufptr;
        buflenstart = buflen;
        /* We parse two times. First we do a simple pass to calculate the
         * csv field count */
        ret = flb_csv_parse_record(&state->state, &bufptr, &buflen,
                &field_count);
        if (ret) {
            break;
        }

        if (write_avro_value(ctx, state, payload_array)) {
            return FLB_FILTER_NOTOUCH;
        }
        
        flb_plg_trace(state->ctx->ins, "=== csv record end ===");
        state->record_field_index = 0;
    }

    if (ret == FLB_CSV_EOF) {
        /* move the incomplete csv record to the beginning of the buffer */
        memmove(state->row_buffer, bufptrstart, buflenstart);
        flb_sds_len_set(state->row_buffer, buflenstart);
    } else {
        flb_sds_len_set(state->row_buffer, 0);
    }

    return 0;
}

static int pack_avro(
        struct filter_avro *ctx,
        struct filter_avro_tag_state *state,
        avro_value_t *logev)

{
    ssize_t payload_size;
    struct flb_time t = {0};
    char writebuf[1024];
    mpack_writer_t writer;

    payload_size = serialize_avro_value(ctx, logev);
    if (payload_size < 0) {
        return -1;
    }

    mpack_writer_init(&writer, writebuf, sizeof(writebuf));
    mpack_writer_set_context(&writer, ctx);
    mpack_writer_set_flush(&writer, mpack_buffer_flush);

    mpack_write_tag(&writer, mpack_tag_array(2));
    flb_time_append_to_mpack(&writer, &t, 0);
    mpack_write_tag(&writer, mpack_tag_map(1));
    mpack_write_cstr(&writer, "avro");
    mpack_write_bin(&writer, ctx->avro_write_buffer, payload_size);

    avro_writer_memory_set_dest(ctx->awriter, ctx->avro_write_buffer,
            ctx->avro_write_buffer_size);

    mpack_writer_flush_message(&writer);
    mpack_writer_destroy(&writer);

    return 0;
}

static int cb_avro_filter(const void *data, size_t bytes,
                         const char *tag, int tag_len,
                         void **out_buf, size_t *out_bytes,
                         struct flb_filter_instance *f_ins,
                         struct flb_input_instance *i_ins,
                         void *filter_context,
                         struct flb_config *config)
{
    int ret;
    struct filter_avro *ctx;
    struct filter_avro_tag_state *state;
    char *outbuf;
    avro_value_t logev;
    avro_value_t value;

    ctx = filter_context;
    flb_sds_len_set(ctx->packbuf, 0);

    state = get_tag_state(ctx, data, bytes);

    if (!state) {
        flb_plg_error(ctx->ins, "max number of tag exceeded");
        return FLB_FILTER_NOTOUCH;
    }

    ret = collect_lines(ctx, state, data, bytes);
    if (ret) {
        return ret;
    }

    if (avro_generic_value_new(ctx->logev_class, &logev)) {
        return -1;
    }

    if (avro_value_get_by_name(&logev, "payload", &value, NULL)) {
        flb_plg_error(ctx->ins, "failed to get avro payload array: %s", avro_strerror());
        return -1;
    }

    ret = csv_to_avro(ctx, state, &value);

    if (ret) {
        avro_value_decref(&logev);
        return ret;
    }

    if (avro_value_get_by_name(&logev, "metadata", &value, NULL)) {
        flb_plg_error(ctx->ins, "failed to get avro metadata: %s", avro_strerror());
        return -1;
    }

    ret = metadata_to_avro(ctx, state, &value);

    if (ret) {
        avro_value_decref(&logev);
        return ret;
    }

    if (avro_value_get_by_name(&logev, "max_size", &value, NULL)) {
        flb_plg_error(ctx->ins, "failed to get avro metadata: %s", avro_strerror());
        return -1;
    }

    if (avro_value_set_int(&value, ctx->payloads_total_size)) {
        return -1;
    }

    if (avro_value_get_by_name(&logev, "avro_schema", &value, NULL)) {
        flb_plg_error(ctx->ins, "failed to get avro schema: %s", avro_strerror());
        return -1;
    }

    if (avro_value_set_string_len(&value, state->avro_schema_json,
                flb_sds_len(state->avro_schema_json) + 1)) {
        flb_plg_error(ctx->ins, "failed to set avro schema: %s", avro_strerror());
        return -1;
    }

    if (pack_avro(ctx, state, &logev)) {
        avro_value_decref(&logev);
        flb_plg_error(ctx->ins, "failed to pack logev avro object");
        return FLB_FILTER_NOTOUCH;
    }
    avro_value_decref(&logev);

    /* allocate outbuf that contains the modified chunks */
    outbuf = flb_malloc(flb_sds_len(ctx->packbuf));
    if (!outbuf) {
        flb_plg_error(ctx->ins, "failed to allocate outbuf");
        return FLB_FILTER_NOTOUCH;
    }
    memcpy(outbuf, ctx->packbuf, flb_sds_len(ctx->packbuf));
    /* link new buffer */
    *out_buf   = outbuf;
    *out_bytes = flb_sds_len(ctx->packbuf);
    return FLB_FILTER_MODIFIED;
}

static int cb_avro_exit(void *data, struct flb_config *config)
{
    int i;
    struct filter_avro *ctx = data;

    for (i = 0; i < FILTER_AVRO_MAX_TAG_COUNT; i++) {
        struct filter_avro_tag_state *state = ctx->states + i;
        if (state->used) {
            flb_csv_destroy(&state->state);
            flb_free(state->tag);
            flb_sds_destroy(state->row_buffer);
            flb_sds_destroy(state->avro_schema_json);
            avro_value_decref(&state->record);
            avro_value_iface_decref(state->aclass);
            avro_schema_decref(state->aschema);
        }
    }

    flb_free(ctx->avro_write_buffer);
    avro_writer_free(ctx->awriter);
    avro_value_iface_decref(ctx->logev_class);
    avro_schema_decref(ctx->logev_schema);

    avro_value_decref(&ctx->meta_value);
    avro_value_iface_decref(ctx->meta_class);
    avro_schema_decref(ctx->meta_schema);

    flb_sds_destroy(ctx->packbuf);
    flb_free(ctx);

    return 0;
}

static struct flb_config_map config_map[] = {
    {
     FLB_CONFIG_MAP_BOOL, "convert_to_avro", "false",
     0, FLB_TRUE, offsetof(struct filter_avro, convert_to_avro),
     "If enabled, convert CSV records into avro, using tag path to find schema "
    },
    {0}
};

struct flb_filter_plugin filter_avro_plugin = {
    .name         = "avro",
    .description  = "AVRO filter plugin",
    .cb_init      = cb_avro_init,
    .cb_filter    = cb_avro_filter,
    .cb_exit      = cb_avro_exit,
    .config_map   = config_map,
    .flags        = 0
};