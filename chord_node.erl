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
-export([start/1, init/0, stabilize/1, fix_fingers/4]).

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
      {SuccessorPIDHashValue, SuccessorPID} = {State#state.successorHashValue, State#state.successorPID},
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
  io:fwrite("Current node ka hash hai ~p aur PID hai ~p ~n", [State#state.id, self()]),
  io:fwrite("Finger table is ~p for node ~p ~n", [State#state.finger_table, self()]),
  io:fwrite("Successor is ~p for ~p ~n", [State#state.successorHashValue, self()]),
  io:fwrite("Successor PID is ~p for ~p ~n", [State#state.successorPID, self()]),
  io:fwrite("Predecessor node is ~p for ~p ~n", [State#state.predecessorHashValue, self()]),
  io:fwrite("Predecessor PID is ~p for ~p ~n", [State#state.predecessorPID, self()]),
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
      start(UpdatedState);
    {_, stablilze} ->
      SuccPID = State#state.successorPID,
      SuccPID ! {self(), get_predecessor},
      start(State);
    {Sender, get_predecessor} ->
      Pred = State#state.predecessorHashValue,
      PredPID = State#state.predecessorPID,
      Sender ! {self(), predecessor, Pred, PredPID},
      start(State);
    {_, predecessor, Pred, PredPID} ->
      UpdatedState = handle_stabilize(State, Pred, PredPID),
      SuccPID = UpdatedState#state.successorPID,
      SuccPID ! {self(), notify, State#state.id, self()},
      start(UpdatedState);
    {_, notify, N, NPID} ->
      UpdatedState = notify(State, N, NPID),
      start(UpdatedState);
    {fix_ith_fingers, IndexToFix} ->
      io:fwrite("Received Message: fix ith finger message recevied on node: ~p and index: ~p~n", [self(), IndexToFix]),
      if
        IndexToFix == 1 ->
          FirstFingerTableData = {State#state.successorHashValue, State#state.successorPID},
          UpdatedMap = maps:put(1, FirstFingerTableData, State#state.finger_table),
          UpdatedState = State#state{finger_table = UpdatedMap},
          fix_finger_actor ! {ith_finger_fixed, IndexToFix},
          start(UpdatedState);
        true ->
          io:fwrite("Received Message: INSIDE ELSE BLOCK OF fix fingers~n"),
          {_, PreviousIndexPID} = maps:get(IndexToFix-1, State#state.finger_table),
          io:fwrite("Previous Index PID ~p~n", [PreviousIndexPID]),
          PreviousIndexPID ! {self(), provide_data_from_finger_table, IndexToFix-1},
          start(State)
      end;
    {Sender, provide_data_from_finger_table, GivenIndex} ->
      io:fwrite("Received Message: Provide data from finger table on node ~p and the index: ~p ~n", [self(), GivenIndex]),
      DataForGivenIndex = maps:get(GivenIndex, State#state.finger_table),
      Sender ! {receive_data_for_index, DataForGivenIndex, GivenIndex + 1},
      start(State);
    {receive_data_for_index, DataForGivenIndex, IndexToUpdate} ->
      UpdatedMap = maps:put(IndexToUpdate, DataForGivenIndex, State#state.finger_table),
      UpdatedState = State#state{finger_table = UpdatedMap},
      fix_finger_actor ! {ith_finger_fixed, IndexToUpdate},
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

stabilize(PIDs) ->
  receive
    {Sender, register_node} ->
      PIDList = [Sender | PIDs],
      stabilize(PIDList);
    {_, stabilizeSelf} ->
      timer:sleep(1000),
      stabilize_nodes(PIDs),
      self() ! {self(),stabilizeSelf},
      stabilize(PIDs)
  end.

stabilize_nodes([]) -> ok;
stabilize_nodes([PID | PIDs]) ->
  PID ! {self(), stablilze},
  stabilize_nodes(PIDs).

handle_stabilize(State, Pred, PredPID) ->
  if
    Pred =/= State#state.id  ->
      UpdatedState = State#state{successorHashValue  = Pred,
      successorPID = PredPID},
      self() ! {update_state, UpdatedState};
    true ->
      UpdatedState = State#state{},
      ok
  end,
  UpdatedState.

notify(State, NewPredecessorHashValue, NewPredecessorPID) ->
  CurrentPredecessor = State#state.predecessorHashValue,
  if CurrentPredecessor == State#state.id->
    UpdatedState = State#state{
      predecessorHashValue = NewPredecessorHashValue,
      predecessorPID = NewPredecessorPID
    };
    true ->
      UpdatedState = State
  end,
  UpdatedState.

fix_ith_fingers_of_all_nodes(_, []) -> ok;
fix_ith_fingers_of_all_nodes(IndexToFix, [Node | Nodes]) ->
  io:fwrite("Sending the fix finger message for node ~p and the index is ~p~n", [Node, IndexToFix]),
  Node ! {fix_ith_fingers, IndexToFix},
  fix_ith_fingers_of_all_nodes(IndexToFix, Nodes).

fix_fingers(NumberOfNodes, NumberOfNodes, IndexBeingFixed, Nodes) ->
  if
    IndexBeingFixed + 1 > ?NUMBER_OF_BITS ->
      ok;
    true ->
      io:fwrite("Reached IN ELSE BLOCK ~n"),
      self() ! {fix_fingers, Nodes},
      fix_fingers(NumberOfNodes, 0, IndexBeingFixed + 1, Nodes)
  end;
fix_fingers(NumberOfNodes, CountOfNodesWithIthFixedFingers, IndexBeingFixed, Nodes) ->
  receive
    {fix_fingers, NodesReceived} ->
      io:fwrite("Received Message: FIX FINGER TABLE ~n"),
      fix_ith_fingers_of_all_nodes(IndexBeingFixed, NodesReceived),
      fix_fingers(NumberOfNodes, CountOfNodesWithIthFixedFingers, IndexBeingFixed, NodesReceived);
    {ith_finger_fixed, IndexBeingFixed} ->
      io:fwrite("Received Message: Index: ~p for some node, CurrentCount: ~p ~n", [IndexBeingFixed, CountOfNodesWithIthFixedFingers+1]),
      fix_fingers(NumberOfNodes, CountOfNodesWithIthFixedFingers + 1, IndexBeingFixed, Nodes)
  end.

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