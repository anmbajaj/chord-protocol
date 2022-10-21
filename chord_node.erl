%%%-------------------------------------------------------------------
%%% @author anmbajaj
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Oct 2022 12:52 AM
%%%-------------------------------------------------------------------
-module(chord_node).
-author("anmbajaj").

%% API
-export([start/0]).

start() ->
  receive
    {abc} -> ok
  end,
  start().
