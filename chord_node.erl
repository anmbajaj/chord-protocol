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

-define(NUMBER_OF_BITS, 16).

-record(state, {
  id,
  predecessorHashValue,
  predecessorPID,
  successorHashValue,
  successorPID,
  finger_table
}).

get_preceding_node(_, _, 0, _) -> self();
get_preceding_node(HashValue, State, CurrentIndex, CurrentPreceding) ->
  {HashValueOfPossiblePreceding, PossiblePrecedingPID} = maps:get(CurrentIndex, State#state.finger_table),
  if
    HashValueOfPossiblePreceding >= State#state.id andalso HashValueOfPossiblePreceding =< HashValue ->
      PossiblePrecedingPID;
    true ->
      get_preceding_node(HashValue, State, CurrentIndex-1, CurrentPreceding)
  end.

closest_preceding_node(HashValue, State) ->
  get_preceding_node(HashValue, State, ?NUMBER_OF_BITS, self()).

find_successor(HashValue, NewNodePID, State) ->
  if
    HashValue >= State#state.id andalso HashValue =< State#state.successorHashValue ->
      {SuccessorPIDHashValue, SuccessorPID} = maps:get(1, State#state.finger_table),
      io:fwrite("Found the successor for ~p, Successor PID HashValue: ~p and Successor PID: ~p...... Sendinf the message of found_successor_update_node~n", [NewNodePID, SuccessorPIDHashValue, SuccessorPIDHashValue]),
      self() ! {found_successor_update_node, NewNodePID, SuccessorPIDHashValue, SuccessorPID};
    true ->
      io:fwrite("Fidning the successor for ~p~n",[NewNodePID]),
      PrecedingNodePID = closest_preceding_node(HashValue, State),
      SelfPIDString = pid_to_list(self()),
      PrecedingNodePIDString = pid_to_list(PrecedingNodePID),
      if
        SelfPIDString == PrecedingNodePIDString ->
          io:fwrite("PID: ~p itself is the succcessor of self: ~p~n", [PrecedingNodePID, self()]),
          io:fwrite("Found the successor for ~p, Successor PID HashValue: ~p and Successor PID: ~p...... Sendinf the message of found_successor_update_node~n", [NewNodePID, State#state.id, self()]),
          self() ! {found_successor_update_node, NewNodePID, State#state.id, self()};
        true ->
          io:fwrite("Preceding node found....  PID: ~p Lets query it for successor", [PrecedingNodePID]),
          io:fwrite("Found the successor for ~p, Successor PID HashValue: ~p and Successor PID: ~p...... Sendinf the message of found_successor_update_node~n", [NewNodePID, State#state.id, self()]),
          PrecedingNodePID ! {find_successor, HashValue, NewNodePID}
      end
  end.

start(#state{
  id = HashValue,
  predecessorHashValue = PredecessorHashValue,
  predecessorPID = PredecessorPID,
  successorHashValue = SuccessorHashValue,
  successorPID = SuccessorPID,
  finger_table = FingerTable
} = State) ->
  %io:fwrite("Created chord node ~p with state ~p", [self(), State]),
  %io:fwrite("~nWaiting for message...... ~n"),
  receive
    {abc} ->
      _ = HashValue,
      _ = PredecessorHashValue,
      _ = PredecessorPID,
      _ = SuccessorHashValue,
      _ = SuccessorPID,
      _ = FingerTable,
      _ = State;
    {_, add_new_node, NewNodePID} ->
      io:fwrite("Received Message: Add new node ~p to the network ~n", [NewNodePID]),
      NewNodePIDHashValue = get_hash_id(pid_to_list(NewNodePID)),
      io:fwrite("Received Message: New node hash value is ~p ", [NewNodePIDHashValue]),
      %SelfPID = self(),
      %self() ! {find_successor, NewNodePIDHashValue, NewNodePID},
      self() ! {find_successor, NewNodePIDHashValue, NewNodePID},
      start(State);
    {find_successor, NewNodePIDHashValue, NewNodePID} ->
      io:fwrite("Do nothing!!!  ~p ~p ~n",[NewNodePIDHashValue, NewNodePID]),
      find_successor(NewNodePIDHashValue, NewNodePID, State),
      start(State);
    {found_successor_update_node, PIDOfNodeToUpdate, SuccessorHashValueToUpdate, SuccessorPIDToUpdate} ->
      io:fwrite("New node ~p successor found and Successor hashvalue: ~p and SuccessorPID: ~p ~n", [PIDOfNodeToUpdate, SuccessorHashValueToUpdate, SuccessorPIDToUpdate]),
      PIDOfNodeToUpdate ! {update_successor, SuccessorHashValueToUpdate, SuccessorPIDToUpdate},
      start(State);
    {update_successor, SuccessorHashValueToUpdate, SuccessorPIDToUpdate} ->
      UpdatedState = State#state{successorHashValue = SuccessorHashValueToUpdate, successorPID = SuccessorPIDToUpdate},
      io:format("Received udpated successor request Node ~p successor updated to ~p ~n", [self(), UpdatedState#state.successorPID]),
      SuccessorPIDToUpdate ! {update_predecessor, State#state.id, self()},
      start(UpdatedState);
    {update_predecessor, PredecessorHashValueToUpdate, PredecessorPIDToUpdate} ->
      UpdatedState = State#state{predecessorHashValue = PredecessorHashValueToUpdate, predecessorPID = PredecessorPIDToUpdate},
      io:format("Received udpated predecessor request Node ~p predecssor updated to ~p ~n", [self(), UpdatedState#state.predecessorPID]),
      start(UpdatedState)
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
    predecessorHashValue = HashValue,
    predecessorPID = self(),
    successorHashValue = HashValue,
    successorPID = self(),
    finger_table = FingerTable
  },
  io:format("State for Node ~p is ~p~n",[self(), State]),
  start(State).