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
#include <fluent-bit/flb_info.h>
#include <fluent-bit/flb_compat.h>
#include <fluent-bit/flb_input.h>
#include <fluent-bit/flb_input_plugin.h>
#include <fluent-bit/flb_luajit.h>
#include <fluent-bit/flb_utils.h>
#include <fluent-bit/flb_pack.h>
#include <fluent-bit/flb_sds.h>
#include <fluent-bit/flb_time.h>

#include "fluent-bit/flb_input_chunk.h"
#include "fluent-bit/flb_lib.h"
#include "in_lua_config.h"
#include "lua.h"

/* Push timestamp as a Lua table into the stack */
static void lua_pushtimetable(lua_State *l, struct flb_time *tm)
{
    lua_createtable(l, 0, 2);

    /* seconds */
    lua_pushlstring(l, "sec", 3);
    lua_pushinteger(l, tm->tm.tv_sec);
    lua_settable(l, -3);

    /* nanoseconds */
    lua_pushlstring(l, "nsec", 4);
    lua_pushinteger(l, tm->tm.tv_nsec);
    lua_settable(l, -3);
}
/*
 * This function is to call lua function table.maxn.
 * CAUTION: table.maxn is removed from Lua 5.2.
 * If we update luajit which is based Lua 5.2+,
 * this function should be removed.
*/
static int lua_isinteger(lua_State *L, int index);
static int lua_table_maxn(lua_State *l)
{
#if defined(LUA_VERSION_NUM) && LUA_VERSION_NUM < 520
    int ret = -1;
    if (lua_type(l, -1) != LUA_TTABLE) {
        return -1;
    }

    lua_getglobal(l, "table");
    lua_getfield(l, -1, "maxn");
    lua_remove(l, -2);    /* remove table (lua_getglobal(L, "table")) */
    lua_pushvalue(l, -2); /* copy record to top of stack */
    ret = lua_pcall(l, 1, 1, 0);
    if (ret < 0) {
        flb_error("[filter_lua] failed to exec table.maxn ret=%d", ret);
        return -1;
    }
    if (lua_type(l, -1) != LUA_TNUMBER) {
        flb_error("[filter_lua] not LUA_TNUMBER");
        lua_pop(l, 1);
        return -1;
    }

    if (lua_isinteger(l, -1)) {
        ret = lua_tointeger(l, -1);
    }
    lua_pop(l, 1);

    return ret;
#else
    return (int)lua_rawlen(l, 1);
#endif
}

static int lua_arraylength(lua_State *l)
{
    lua_Integer n;
    int count = 0;
    int max = 0;
    int ret = 0;

    ret = lua_table_maxn(l);
    if (ret > 0) {
        return ret;
    }

    lua_pushnil(l);
    while (lua_next(l, -2) != 0) {
        if (lua_type(l, -2) == LUA_TNUMBER) {
            n = lua_tonumber(l, -2);
            if (n > 0) {
                max = n > max ? n : max;
                count++;
                lua_pop(l, 1);
                continue;
            }
        }
        lua_pop(l, 2);
        return -1;
    }
    if (max != count)
        return -1;
    return max;
}

static void lua_tomsgpack(struct lua_input *li, msgpack_packer *pck, int index);
static void lua_toarray(struct lua_input *li, msgpack_packer *pck, int index)
{
    int len;
    int i;
    lua_State *l = li->lua->state;

    lua_pushnumber(l, (lua_Number)lua_objlen(l, -1)); // lua_len
    len = (int)lua_tointeger(l, -1);
    lua_pop(l, 1);

    msgpack_pack_array(pck, len);
    for (i = 1; i <= len; i++) {
        lua_rawgeti(l, -1, i);
        lua_tomsgpack(li, pck, 0);
        lua_pop(l, 1);
    }
}
static void try_to_convert_data_type(struct lua_input *li,
                                     msgpack_packer *pck,
                                     int index)
{
    size_t   len;
    const char *tmp = NULL;
    lua_State *l = li->lua->state;

    struct mk_list  *tmp_list = NULL;
    struct mk_list  *head     = NULL;
    struct l2c_type *l2c      = NULL;

    // convert to int
    if ((lua_type(l, -2) == LUA_TSTRING)
        && lua_type(l, -1) == LUA_TNUMBER){
        tmp = lua_tolstring(l, -2, &len);

        mk_list_foreach_safe(head, tmp_list, &li->l2c_types) {
            l2c = mk_list_entry(head, struct l2c_type, _head);
            if (!strncmp(l2c->key, tmp, len) && l2c->type == L2C_TYPE_INT) {
                lua_tomsgpack(li, pck, -1);
                msgpack_pack_int64(pck, (int64_t)lua_tonumber(l, -1));
                return;
            }
        }
    }
    else if ((lua_type(l, -2) == LUA_TSTRING)
             && lua_type(l, -1) == LUA_TTABLE){
        tmp = lua_tolstring(l, -2, &len);

        mk_list_foreach_safe(head, tmp_list, &li->l2c_types) {
            l2c = mk_list_entry(head, struct l2c_type, _head);
            if (!strncmp(l2c->key, tmp, len) && l2c->type == L2C_TYPE_ARRAY) {
                lua_tomsgpack(li, pck, -1);
                lua_toarray(li, pck, 0);
                return;
            }
        }
    }

    /* not matched */
    lua_tomsgpack(li, pck, -1);
    lua_tomsgpack(li, pck, 0);
}

static int lua_isinteger(lua_State *L, int index)
{
    lua_Number n;
    lua_Integer i;

    if (lua_type(L, index) == LUA_TNUMBER) {
        n = lua_tonumber(L, index);
        i = lua_tointeger(L, index);

        if (i == n) {
            return 1;
        }
    }
    return 0;
}

static void lua_tomsgpack(struct lua_input *li, msgpack_packer *pck, int index)
{
    int len;
    int i;
    lua_State *l = li->lua->state;

    switch (lua_type(l, -1 + index)) {
        case LUA_TSTRING:
            {
                const char *str;
                size_t len;

                str = lua_tolstring(l, -1 + index, &len);

                msgpack_pack_str(pck, len);
                msgpack_pack_str_body(pck, str, len);
            }
            break;
        case LUA_TNUMBER:
            {
                if (lua_isinteger(l, -1 + index)) {
                    int64_t num = lua_tointeger(l, -1 + index);
                    msgpack_pack_int64(pck, num);
                }
                else {
                    double num = lua_tonumber(l, -1 + index);
                    msgpack_pack_double(pck, num);
                }
            }
            break;
        case LUA_TBOOLEAN:
            if (lua_toboolean(l, -1 + index))
                msgpack_pack_true(pck);
            else
                msgpack_pack_false(pck);
            break;
        case LUA_TTABLE:
            len = lua_arraylength(l);
            if (len > 0) {
                msgpack_pack_array(pck, len);
                for (i = 1; i <= len; i++) {
                    lua_rawgeti(l, -1, i);
                    lua_tomsgpack(li, pck, 0);
                    lua_pop(l, 1);
                }
            } else
            {
                len = 0;
                lua_pushnil(l);
                while (lua_next(l, -2) != 0) {
                    lua_pop(l, 1);
                    len++;
                }
                msgpack_pack_map(pck, len);

                lua_pushnil(l);

                if (li->l2c_types_num > 0) {
                    /* type conversion */
                    while (lua_next(l, -2) != 0) {
                        try_to_convert_data_type(li, pck, index);
                        lua_pop(l, 1);
                    }
                } else {
                    while (lua_next(l, -2) != 0) {
                        lua_tomsgpack(li, pck, -1);
                        lua_tomsgpack(li, pck, 0);
                        lua_pop(l, 1);
                    }
                }
            }
            break;
        case LUA_TNIL:
            msgpack_pack_nil(pck);
            break;

         case LUA_TLIGHTUSERDATA:
            if (lua_touserdata(l, -1 + index) == NULL) {
                msgpack_pack_nil(pck);
                break;
            }
         case LUA_TFUNCTION:
         case LUA_TUSERDATA:
         case LUA_TTHREAD:
           /* cannot serialize */
           break;
    }
}

/* cb_collect callback */
static int in_lua_collect(struct flb_input_instance *ins,
                          struct flb_config *config, void *in_context)
{
    int ret;
    struct lua_input *ctx = in_context;
    /* Lua return values */
    struct flb_time t;
    msgpack_packer pck;
    msgpack_sbuffer sbuf;

    /* Get timestamp */
    flb_time_get(&t);

    /* Prepare function call, pass 1 argument (timestamp), expect 2 return values */
    lua_getglobal(ctx->lua->state, ctx->call);

    /* push timestamp */
    lua_pushtimetable(ctx->lua->state, &t);

    ret = lua_pcall(ctx->lua->state, 1, 2, 0);
    if (ret != 0) {
        flb_plg_error(ctx->ins, "error code %d: %s",
                ret, lua_tostring(ctx->lua->state, -1));
        lua_pop(ctx->lua->state, 1);
        msgpack_sbuffer_destroy(&sbuf);
        return -1;
    }

    msgpack_sbuffer_init(&sbuf);
    msgpack_packer_init(&pck, &sbuf, msgpack_sbuffer_write);
    msgpack_pack_array(&pck, 2);

    if (lua_type(ctx->lua->state, -2) == LUA_TTABLE) {
        /* Retrieve seconds */
        lua_getfield(ctx->lua->state, -2, "sec");
        t.tm.tv_sec = lua_tointeger(ctx->lua->state, -1);
        lua_pop(ctx->lua->state, 1);

        /* Retrieve nanoseconds */
        lua_getfield(ctx->lua->state, -2, "nsec");
        t.tm.tv_nsec = lua_tointeger(ctx->lua->state, -1);
        lua_pop(ctx->lua->state, 1);
    }
    else {
        flb_plg_error(ctx->ins, "invalid lua timestamp type returned");
    }
    flb_time_append_to_msgpack(&t, &pck, 0);

    lua_tomsgpack(ctx, &pck, 0);
    lua_pop(ctx->lua->state, 2);

    flb_input_chunk_append_raw(ins, NULL, 0, sbuf.data, sbuf.size);
    return 0;
}

static int is_valid_func(lua_State *lua, flb_sds_t func)
{
    int ret = FLB_FALSE;

    lua_getglobal(lua, func);
    if (lua_isfunction(lua, -1)) {
        ret = FLB_TRUE;
    }
    lua_pop(lua, -1); /* discard return value of isfunction */

    return ret;
}

/* Initialize plugin */
static int in_lua_init(struct flb_input_instance *in,
                       struct flb_config *config, void *data)
{
    int ret;
    (void) data;
    struct lua_input *ctx;
    struct flb_luajit *lj;

    /* Create context */
    ctx = lua_config_create(in, config);
    if (!ctx) {
        flb_error("[in_lua] filter cannot be loaded");
        return -1;
    }

    /* Create LuaJIT state/vm */
    lj = flb_luajit_create(config);
    if (!lj) {
        lua_config_destroy(ctx);
        return -1;
    }
    ctx->lua = lj;

    /* Load Script */
    ret = flb_luajit_load_script(ctx->lua, ctx->script);
    if (ret == -1) {
        lua_config_destroy(ctx);
        return -1;
    }
    lua_pcall(ctx->lua->state, 0, 0, 0);

    if (is_valid_func(ctx->lua->state, ctx->call) != FLB_TRUE) {
        flb_plg_error(ctx->ins, "function %s is not found", ctx->call);
        lua_config_destroy(ctx);
        return -1;
    }

    /* Set context */
    flb_input_set_context(in, ctx);

    ret = flb_input_set_collector_time(in,
                                       in_lua_collect,
                                       1,
                                       0, config);

    return 0;
}

static int in_lua_exit(void *data, struct flb_config *config)
{
    struct lua_input *ctx;

    ctx = data;
    flb_luajit_destroy(ctx->lua);
    lua_config_destroy(ctx);

    return 0;
}


/* Configuration properties map */
static struct flb_config_map config_map[] = {
    {
     FLB_CONFIG_MAP_STR, "script", NULL,
     0, FLB_FALSE, 0,
     "The path of lua script."
    },
    {
     FLB_CONFIG_MAP_STR, "call", NULL,
     0, FLB_TRUE, offsetof(struct lua_input, call),
     "Lua function name that will be triggered to do filtering."
    },
    {
     FLB_CONFIG_MAP_STR, "type_int_key", NULL,
     0, FLB_FALSE, 0,
     "If these keys are matched, the fields are converted to integer. "
     "If more than one key, delimit by space."
    },
    {
     FLB_CONFIG_MAP_STR, "type_array_key", NULL,
     0, FLB_FALSE, 0,
     "If these keys are matched, the fields are converted to array. "
     "If more than one key, delimit by space."
    },
    {0}
};


struct flb_input_plugin in_lua_plugin = {
    .name         = "lua",
    .description  = "Use a Lua script to generate input",
    .cb_init      = in_lua_init,
    .cb_pre_run   = NULL,
    .cb_collect   = in_lua_collect,
    .cb_flush_buf = NULL,
    .config_map   = config_map,
    .cb_exit      = in_lua_exit
};
