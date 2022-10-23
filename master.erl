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

add_nodes_to_network([], _) ->
  io:fwrite("All nodes sent for addition to the network ~n");
add_nodes_to_network([Node | Nodes], InNetworkNode) ->
  InNetworkNode ! {self(), add_new_node, Node},
  timer:sleep(3000),
  add_nodes_to_network(Nodes, InNetworkNode).

build_p2p_network([Node | Nodes]) ->
  add_nodes_to_network(Nodes, Node).

create_nodes(0, Acc, _, _) -> Acc;
create_nodes(NumberOfNodesYetToSpawn, Acc, StabilizerPID, FixFingerPID) ->
  PID = spawn(?CHORD_NODE, init, []),
  io:fwrite("PID created is ~p~n", [PID]),
  StabilizerPID ! {PID, register_node},
  FixFingerPID ! {PID, register_node},
  create_nodes(NumberOfNodesYetToSpawn-1, [PID | Acc], StabilizerPID, FixFingerPID).

start_master() ->
  receive
    {start_chord, NumberOfNodes, NumberOfRequests} ->
      io:fwrite("Received start chord message... Creating the nodes now ~n"),
      _ = NumberOfRequests,
      StabilizerPID = spawn(?CHORD_NODE, stabilize, [[]]),
      FixFingerPID = spawn(?CHORD_NODE, fix_finger_tables, [[]]),
      Nodes = create_nodes(NumberOfNodes, [], StabilizerPID, FixFingerPID),
      StabilizerPID ! {self(), stabilizeSelf},
      FixFingerPID ! {self(), fix_finger, 1},
      build_p2p_network(Nodes);
    {Sender, ok, Message} ->
      io:fwrite("Received Message: From: ~p, Message: ~p~n", [Sender, Message])
  end,
  start_master().

start(NumberOfNodes, NumberOfRequests) ->
  PID = spawn(?MODULE, start_master, []),
  %register(?MASTER, PID),
  PID ! {start_chord, NumberOfNodes, NumberOfRequests}.