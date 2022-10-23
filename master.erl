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
-export([start/2, start_master/4]).

-define(NUMBER_OF_BITS, 16).
-define(MASTER, master).
-define(CHORD_NODE, chord_node).
-define(RANDOM_STRING_LENGTH, 10).
-define(ALLOWED_CHARS, "qwertyQWERTY1234567890").

look_up_data(_, [], _, _) -> ok;
look_up_data(Data, [Node | Nodes], NumberOfRequests, NumberOfNodes) ->
  Node ! {self(), lookup_data, Data, NumberOfRequests, NumberOfNodes},
  look_up_data(Data, Nodes, NumberOfRequests, NumberOfNodes).

store_the_data(_ , []) -> ok;
store_the_data(Data, [Node | Nodes]) ->
  Node ! {dataset, Data},
  store_the_data(Data, Nodes).

get_random_string(Length, AllowedChars) ->
  lists:foldl(fun(_, Acc) ->
    [lists:nth(rand:uniform(length(AllowedChars)),
      AllowedChars)]
    ++ Acc
              end, [], lists:seq(1, Length)).

generate_data(Data, NumberOfRequests, NumberOfRequests) -> Data;
generate_data(Data, Count, NumberOfRequests ) ->
  RandomString = get_random_string(?RANDOM_STRING_LENGTH, ?ALLOWED_CHARS),
  <<SHA256Value:256>> = crypto:hash(sha256, [RandomString]),
  FinalHashString = SHA256Value rem trunc(math:pow(2, ?NUMBER_OF_BITS)),
  %io:fwrite("Anmol Bajaj Final Hash is ~p ~n", [FinalHashString]),
  NewData = maps:put(RandomString, FinalHashString, Data),
  generate_data(NewData, Count + 1, NumberOfRequests).

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
  StabilizerPID ! {PID, register_node},
  %FixFingerPID ! {PID, register_node},
  create_nodes(NumberOfNodesYetToSpawn-1, [PID | Acc], StabilizerPID, FixFingerPID).

start_master(NodesExecuted, SumOfCount, TotalNodes, TotalRequests) ->
  receive
    {start_chord, NumberOfNodes, NumberOfRequests} ->
      %io:fwrite("Received start chord message... Creating the nodes now ~n"),
      EmptyDataMap = maps:new(),
      Data = generate_data(EmptyDataMap, 0, NumberOfRequests),
      StabilizerPID = spawn(?CHORD_NODE, stabilize, [[]]),
      FixFingerPID = spawn(?CHORD_NODE, fix_fingers, [NumberOfNodes, 0, 1, []]),
      register(fix_finger_actor, FixFingerPID),
      Nodes = create_nodes(NumberOfNodes, [], StabilizerPID, FixFingerPID),
      StabilizerPID ! {self(), stabilizeSelf},
      build_p2p_network(Nodes),
      timer:sleep(NumberOfNodes*5000),
      fix_finger_actor ! {fix_fingers, Nodes},
      store_the_data(Data, Nodes),
      timer:sleep(10000),
      look_up_data(Data, Nodes, NumberOfRequests, NumberOfNodes),
      start_master(NodesExecuted, SumOfCount, NumberOfNodes, NumberOfRequests);
    {Sender, ok, Message} ->
      io:fwrite("Received Message: From: ~p, Message: ~p~n", [Sender, Message]),
      start_master(NodesExecuted, SumOfCount, TotalNodes, TotalRequests);
    {lookup_successful} ->
      start_master(NodesExecuted + 1, SumOfCount, TotalNodes, TotalRequests);
    {found_data, Count} ->
      io:fwrite("Count received is : ~p~n", [Count]),
      io:fwrite("Received messaged at master~n"),
      NewSum = SumOfCount + Count,
      if NodesExecuted == TotalNodes ->
        Average = NewSum / TotalRequests,
        io:fwrite("Average Hops: ~p ~n", [Average]),
        exit(self());
        true ->
          ok
      end,
      start_master(NodesExecuted+1, NewSum, TotalNodes, TotalRequests)
  end,
  start_master(NodesExecuted, SumOfCount, TotalNodes, TotalRequests).

start(NumberOfNodes, NumberOfRequests) ->
  PID = spawn(?MODULE, start_master, [0,0, NumberOfNodes, NumberOfRequests]),
  %register(?MASTER, PID),
  PID ! {start_chord, NumberOfNodes, NumberOfRequests}.