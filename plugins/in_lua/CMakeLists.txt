set(src
  in_lua_config.c
  in_lua.c)

if(MSVC)
  FLB_PLUGIN(in_lua "${src}" "")
else()
  FLB_PLUGIN(in_lua "${src}" "m")
endif()
