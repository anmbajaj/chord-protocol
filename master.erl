%%%-------------------------------------------------------------------
%%% @author anmbajaj
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Oct 2022 8:47 PM
%%%-------------------------------------------------------------------
-module(master).
-author("anmbajaj").

%% API
-export([start/2, start_master/0]).

-define(NUMBER_OF_BITS, 256).
-define(MASTER, master).
-define(CHORD_NODE, chord_node).

get_hash_id(Number) ->
  NodeString = integer_to_list(Number),
  io_lib:format("~64.16.0b", [binary:decode_unsigned(crypto:hash(sha256, [NodeString]))]).

create_nodes(0, _, _, Acc) -> Acc;
create_nodes(NumberOfNodesYetToSpawn, TotalNumberOfNodes, TotalNumberOfRequests, Acc) ->
  SelfHashedID = get_hash_id(NumberOfNodesYetToSpawn),
  io:fwrite("Actor created with node value ~p and hashed valued ~p~n", [NumberOfNodesYetToSpawn, SelfHashedID]),
  PID = spawn(?CHORD_NODE, start, []),
  NodeTuple = {PID, SelfHashedID},
  io:fwrite("Node Tuple created ~p~n", [NodeTuple]),
  create_nodes(NumberOfNodesYetToSpawn-1, TotalNumberOfNodes, TotalNumberOfRequests, [NodeTuple | Acc]).

start_master() ->
  receive
    {start_chord, NumberOfNodes, NumberOfRequests} ->
      io:fwrite("Received start chord message... Creating the nodes now ~n"),
      create_nodes(NumberOfNodes, NumberOfNodes, NumberOfRequests, [])
  end,
  start_master().

start(NumberOfNodes, NumberOfRequests) ->
  PID = spawn(?MODULE, start_master, []),
  register(?MASTER, PID),
  PID ! {start_chord, NumberOfNodes, NumberOfRequests}.