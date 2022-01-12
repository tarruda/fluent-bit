/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  Fluent Bit
 *  ==========
 *  Copyright (C) 2019-2021 The Fluent Bit Authors
 *  Copyright (C) 2015-2018 Treasure Data Inc.
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

#include <fluent-bit/flb_input_plugin.h>
#include <fluent-bit/flb_config.h>
#include <fluent-bit/flb_pack.h>
#include <fluent-bit/flb_engine.h>
#include <fluent-bit/flb_time.h>
#include <fluent-bit/flb_parser.h>
#include <fluent-bit/flb_error.h>
#include <fluent-bit/flb_utils.h>
#include <fluent-bit/flb_input_thread.h>
#include <mpack/mpack.h>
#include <stddef.h>

#include "fluent-bit/flb_input.h"
#include "in_kafka.h"
#include "rdkafka.h"

static void process_message(mpack_writer_t *writer, rd_kafka_message_t *rkm)
{
    struct flb_time t;

    mpack_write_tag(writer, mpack_tag_array(2));

    flb_time_get(&t);
    flb_time_append_to_mpack(writer, &t, 0);

    mpack_write_tag(writer, mpack_tag_map(6));

    mpack_write_cstr(writer, "topic");
    if (rkm->rkt) {
        mpack_write_cstr(writer, rd_kafka_topic_name(rkm->rkt));
    } else {
        mpack_write_nil(writer);
    }

    mpack_write_cstr(writer, "partition");
    mpack_write_i32(writer, rkm->partition);

    mpack_write_cstr(writer, "offset");
    mpack_write_i64(writer, rkm->offset);

    mpack_write_cstr(writer, "error");
    if (rkm->err) {
        mpack_write_cstr(writer, rd_kafka_message_errstr(rkm));
    } else {
        mpack_write_nil(writer);
    }

    mpack_write_cstr(writer, "key");
    if (rkm->key) {
        mpack_write_str(writer, rkm->key, rkm->key_len);
    } else {
        mpack_write_nil(writer);
    }

    mpack_write_cstr(writer, "payload");
    if (rkm->payload) {
        mpack_write_str(writer, rkm->payload, rkm->len);
    } else {
        mpack_write_nil(writer);
    }

    mpack_writer_flush_message(writer);
}

static void in_kafka_callback(int write_fd, void *data)
{
    struct flb_input_thread *it = data;
    struct flb_in_kafka_config *ctx = data - offsetof(struct flb_in_kafka_config, it);
    mpack_writer_t *writer = &ctx->it.writer;

    while (!flb_input_thread_exited(it)) {
        rd_kafka_message_t *rkm = rd_kafka_consumer_poll(ctx->rk, 500);

        if (rkm) {
            process_message(writer, rkm);
            fflush(ctx->it.write_file);
            rd_kafka_message_destroy(rkm);
            rd_kafka_commit(ctx->rk, NULL, 0);
        }
    }
}

/* Initialize plugin */
static int in_kafka_init(struct flb_input_instance *ins,
                         struct flb_config *config, void *data)
{
    int ret;
    const char *conf;
    struct mk_list *head;
    struct mk_list *topics;
    struct flb_split_entry *entry;
    struct flb_in_kafka_config *ctx;
    rd_kafka_conf_t *kafka_conf = NULL;
    rd_kafka_topic_partition_list_t *kafka_topics = NULL;
    rd_kafka_resp_err_t err;
    char errstr[512];
    (void) data;

    /* Allocate space for the configuration context */
    ctx = flb_malloc(sizeof(struct flb_in_kafka_config));
    if (!ctx) {
        return -1;
    }
    kafka_conf = rd_kafka_conf_new();
    if (!kafka_conf) {
        flb_plg_error(ins, "Could not initialize kafka config object");
        goto init_error;
    }


    conf = flb_input_get_property("client_id", ins);
    if (!conf) {
        conf = "fluent-bit";
    }
    if (rd_kafka_conf_set(kafka_conf, "client.id", conf,
                errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        flb_plg_error(ins, "cannot configure client id: %s", errstr);
        goto init_error;
    }

    conf = flb_input_get_property("group_id", ins);
    if (!conf) {
        conf = "fluent-bit";
    }
    if (rd_kafka_conf_set(kafka_conf, "group.id", conf,
                errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        flb_plg_error(ins, "%s", errstr);
        goto init_error;
    }

    conf = flb_input_get_property("brokers", ins);
    if (!conf) {
        flb_plg_error(ins, "config: no brokers defined");
        goto init_error;
    }
    if (rd_kafka_conf_set(kafka_conf, "bootstrap.servers", conf,
                errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        flb_plg_error(ins, "%s", errstr);
        goto init_error;
    }

    ctx->rk = rd_kafka_new(RD_KAFKA_CONSUMER, kafka_conf, errstr,
            sizeof(errstr));

    /* Create Kafka consumer handle */
    if (!ctx->rk) {
        flb_plg_error(ins, "Failed to create new consumer: %s", errstr);
        goto init_error;
    }

    kafka_topics = rd_kafka_topic_partition_list_new(1);
    if (!kafka_topics) {
        flb_plg_error(ins, "Failed to allocate topic list");
        goto init_error;
    }


    conf = flb_input_get_property("topics", ins);
    if (!conf) {
        flb_plg_error(ins, "config: no topics specified");
        goto init_error;
    }
    topics = flb_utils_split(conf, ',', -1);
    if (!topics) {
        flb_plg_error(ins, "config: invalid topics defined \"%s\"", conf);
        goto init_error;
    }

    mk_list_foreach(head, topics) {
        entry = mk_list_entry(head, struct flb_split_entry, _head);
        rd_kafka_topic_partition_list_add(kafka_topics, entry->value, 0);
    }
    flb_utils_split_free(topics);


    if ((err = rd_kafka_subscribe(ctx->rk, kafka_topics))) {
        flb_plg_error(ins, "Failed to start consuming topics: %s", rd_kafka_err2str(err));
        goto init_error;
    }
    rd_kafka_topic_partition_list_destroy(kafka_topics);
    kafka_topics = NULL;

    /* create worker thread */
    ret = flb_input_thread_init(&ctx->it, in_kafka_callback, &ctx->it);
    if (ret) {
        flb_errno();
        flb_plg_error(ins, "Could not initialize worker thread");
        goto init_error;
    }

    /* Set the context */
    flb_input_set_context(ins, &ctx->it);

    /* Collect upon data available on the pipe read fd */
    ret = flb_input_set_collector_event(ins,
                                        flb_input_thread_collect,
                                        ctx->it.read,
                                        config);
    if (ret == -1) {
        flb_plg_error(ins, "Could not set collector for thread dummy input plugin");
        goto init_error;
    }
    ctx->it.coll_fd = ret;

    return 0;

init_error:
    if (kafka_topics) {
        rd_kafka_topic_partition_list_destroy(kafka_topics);
    }
    if (ctx->rk) {
        rd_kafka_destroy(ctx->rk);
    } else if (kafka_conf) {
        // conf is already destroyed when rd_kafka is initialized
        rd_kafka_conf_destroy(kafka_conf);
    }
    flb_free(ctx);

    return -1;
}

/* Cleanup serial input */
static int in_kafka_exit(void *in_context, struct flb_config *config)
{
    struct flb_input_thread *it;
    struct flb_in_kafka_config *ctx;

    if (!in_context) {
        return 0;
    }

    it = in_context;
    ctx = (in_context - offsetof(struct flb_in_kafka_config, it));
    flb_input_thread_destroy(it, ctx->ins);
    rd_kafka_destroy(ctx->rk);
    flb_free(ctx);

    return 0;
}

/* Plugin reference */
struct flb_input_plugin in_kafka_plugin = {
    .name         = "kafka",
    .description  = "Kafka consumer input plugin",
    .cb_init      = in_kafka_init,
    .cb_pre_run   = NULL,
    .cb_collect   = flb_input_thread_collect,
    .cb_flush_buf = NULL,
    .cb_exit      = in_kafka_exit
};
