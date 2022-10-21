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
-export([start/1, init/0]).

-define(NUMBER_OF_BITS, 256).

-record(state, {
  id,
  predecessor,
  successor,
  finger_table
}).

start(#state{
  id = HashValue,
  predecessor = Predecessor,
  successor = Successor,
  finger_table = FingerTable
} = State) ->
  io:fwrite("Created chord node ~p with state ~p", [self(), State]),
  receive
    {abc} ->
      _ = HashValue,
      _ = Predecessor,
      _ = Successor,
      _ = FingerTable,
      _ = State;
    {Sender, add_new_node, Node} ->
      io:fwrite("Received Message: Add new node ~p to the network ~n", [Node]),
      Message = "New node successfully added to the network",
      Sender ! {self(), ok, Message},
      start(State)
  end.

insert_initial_data_in_finger_table(?NUMBER_OF_BITS, _, FingerTable) -> FingerTable;
insert_initial_data_in_finger_table(LastInsertedKey, HashValue, FingerTable) ->
  DataTuple = {HashValue, self()},
  UpdatedFingerTable = maps:put(LastInsertedKey+1, DataTuple, FingerTable),
  insert_initial_data_in_finger_table(LastInsertedKey+1, HashValue, UpdatedFingerTable).

create_initial_finger_table(HashValue) ->
  FingerTable = maps:new(),
  insert_initial_data_in_finger_table(0, HashValue, FingerTable).

get_hash_id(NodePID) ->
  <<SHA256Value:256>> = crypto:hash(sha256, [NodePID]),
  SHA256Value rem trunc(math:pow(2, ?NUMBER_OF_BITS)).

init() ->
  HashValue = get_hash_id(pid_to_list(self())),
  FingerTable = create_initial_finger_table(HashValue),
  State = #state{
    id = HashValue,
    predecessor = nil,
    successor = self(),
    finger_table = FingerTable
  },
  start(State).