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

#ifndef FLB_LUA_CONFIG_H
#define FLB_LUA_CONFIG_H

#include <fluent-bit/flb_info.h>
#include <fluent-bit/flb_input.h>
#include <fluent-bit/flb_luajit.h>
#include <fluent-bit/flb_sds.h>

#define LUA_BUFFER_CHUNK    1024 * 8  /* 8K should be enough to get started */
#define L2C_TYPES_NUM_MAX   16

enum l2c_type_enum {
    L2C_TYPE_INT,
    L2C_TYPE_ARRAY
};

struct l2c_type {
    flb_sds_t key;
    int type;
    struct mk_list _head;
};

struct lua_input {
    flb_sds_t script;                 /* lua script path */
    flb_sds_t call;                   /* function name   */
    int    l2c_types_num;             /* number of l2c_types */
    struct mk_list l2c_types;         /* data types (lua -> C) */
    struct flb_luajit *lua;           /* state context   */
    struct flb_input_instance *ins;   /* input instance */
    msgpack_sbuffer mp_sbuf;
};

struct lua_input *lua_config_create(struct flb_input_instance *ins,
                                    struct flb_config *config);
void lua_config_destroy(struct lua_input *li);

#endif
