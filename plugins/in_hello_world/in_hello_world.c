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

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include <msgpack.h>
#include <fluent-bit/flb_input.h>
#include <fluent-bit/flb_input_plugin.h>
#include <fluent-bit/flb_config.h>
#include <fluent-bit/flb_config_map.h>
#include <fluent-bit/flb_error.h>
#include <fluent-bit/flb_time.h>
#include <fluent-bit/flb_pack.h>

#include "in_hello_world.h"
#include "msgpack/pack.h"


static int in_hello_world_collect(struct flb_input_instance *ins,
                                  struct flb_config *config, void *in_context)
{
    msgpack_sbuffer mp_sbuf;
    msgpack_packer mp_pck;

    msgpack_sbuffer_init(&mp_sbuf);
    msgpack_packer_init(&mp_pck, &mp_sbuf, msgpack_sbuffer_write);

    msgpack_pack_array(&mp_pck, 2);

    flb_pack_time_now(&mp_pck);

    msgpack_pack_map(&mp_pck, 1);
    msgpack_pack_str_with_body(&mp_pck, "message", 7);
    msgpack_pack_str_with_body(&mp_pck, "hello world", 11);

    flb_input_chunk_append_raw(ins, NULL, 0, mp_sbuf.data, mp_sbuf.size);
    msgpack_sbuffer_destroy(&mp_sbuf);
    return 0;
}

static int config_destroy(struct flb_hello_world *ctx)
{
    flb_free(ctx);
    return 0;
}

/* Initialize plugin */
static int in_hello_world_init(struct flb_input_instance *in,
                       struct flb_config *config, void *data)
{
    int ret = -1;
    struct flb_hello_world *ctx = NULL;

    /* Allocate space for the configuration */
    ctx = flb_malloc(sizeof(struct flb_hello_world));
    if (ctx == NULL) {
        return -1;
    }
    ctx->ins = in;

    flb_input_set_context(in, ctx);
    ret = flb_input_set_collector_time(in,
                                       in_hello_world_collect,
                                       1,
                                       0, config);
    if (ret < 0) {
        flb_plg_error(ctx->ins, "could not set collector for hello_world input plugin");
        config_destroy(ctx);
        return -1;
    }

    return 0;
}

static int in_hello_world_exit(void *data, struct flb_config *config)
{
    (void) *config;
    struct flb_hello_world *ctx = data;
    config_destroy(ctx);
    return 0;
}


/* Configuration properties map */
static struct flb_config_map config_map[] = {
   {0}
};


struct flb_input_plugin in_hello_world_plugin = {
    .name         = "hello_world",
    .description  = "Hello world input plugin",
    .cb_init      = in_hello_world_init,
    .cb_pre_run   = NULL,
    .cb_collect   = in_hello_world_collect,
    .cb_flush_buf = NULL,
    .config_map   = config_map,
    .cb_exit      = in_hello_world_exit
};
