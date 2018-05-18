-module(my).
-import(digraph,[graph/0,add_edge/3,add_vertex/2,vertices/1,out_neighbours/2]).
-import(ordsets,[add_element/2]).
-export([start/0, send/4, received/5,bfs/3,sendToList/4]).

send(Message,From,To,Source_VertexID) ->
    [ To ! {Message,From,To,Source_VertexID}],
    io:fwrite("~w send from ~w  to ~w ~n", [Message,From,To]).

sendToList(_,_,[],_) ->
    done;

sendToList(Message,From,Plist,Source_VertexID) ->
    [H|T]=Plist,
    send(Message,From,H,Source_VertexID),
    sendToList(Message,From,T,Source_VertexID).

received(Parent,My_ID,Neighbours,Children,Unrelated) ->
    receive
        {accept,From,To,Source_VertexID} ->
            io:format("ACCEPTED Msg Received to ~w from ~w ~n", [To,From]), 
            % io:format("****************************************** ~n",[]),
            % io:format("************* Parent of  ~w : ~w ************** ~n",[From,To]), 
            % io:format("****************************************** ~n",[]),
            J_set = ordsets:from_list([Source_VertexID]),
            Children1 = ordsets:union(Children,J_set),
            Parent_set = ordsets:from_list([Parent]),
            Children_and_Unrelated = ordsets:union(Children1,Unrelated),
            Parent_less_Neighbours = ordsets:subtract(Neighbours,Parent_set),
            Length1 = ordsets:size(Children_and_Unrelated),
            Length2 = ordsets:size(Parent_less_Neighbours),
            % io:format("Children_and_Unrelated ~w : ~p ~n",[My_ID,Children_and_Unrelated]),
            % io:format("Parent_less_Neighbours ~w : ~p ~n",[My_ID,Parent_less_Neighbours]),
            BoolAns = ordsets:is_subset(Children_and_Unrelated,Parent_less_Neighbours),
            % io:format("L1: ~w  ,L2 :~w  ,BoolAns : ~w ~n", [Length1,Length2,BoolAns]), 
            % io:format("vertex : ~w  L1: ~w  ,L2 :~w ~n", [My_ID,Length1,Length2]), 
            if Length1 == Length2, BoolAns == true ->
                % io:format("Parent of  ~w : ~w ~n",[My_ID,Parent]),
                % io:format("exited normally ~w ~n",[My_ID]);
                exit(normal);
            true ->
                done
            end,
            io:format("Children Set of ~w  :  ~p ~n", [My_ID,Children1]),
            % io:format("Unrelated Set of ~w  :  ~p ~n", [My_ID,Unrelated]),
            received(Parent,My_ID,Neighbours,Children1,Unrelated);  

        {reject,From,To,Source_VertexID} ->
 			io:format("REJECTED Msg Received to ~w from ~w ~n", [To,From]),   
            J_set = ordsets:from_list([Source_VertexID]),
            Unrelated1 = ordsets:union(Unrelated,J_set),
            Parent_set = ordsets:from_list([Parent]),
            Children_and_Unrelated = ordsets:union(Children,Unrelated1),
            Parent_less_Neighbours = ordsets:subtract(Neighbours,Parent_set),
            % io:format("Children_and_Unrelated ~w : ~p ~n",[My_ID,Children_and_Unrelated]),
            % io:format("Parent_less_Neighbours ~w : ~p ~n",[My_ID,Parent_less_Neighbours]),
            Length1 = ordsets:size(Children_and_Unrelated),
            Length2 = ordsets:size(Parent_less_Neighbours),
            BoolAns = ordsets:is_subset(Children_and_Unrelated,Parent_less_Neighbours),
            % io:format("L1: ~w  ,L2 :~w  ,BoolAns : ~w ~n", [Length1,Length2,BoolAns]),  
            % io:format("vertex : ~w  L1: ~w  ,L2 :~w ~n", [My_ID,Length1,Length2]), 
            if Length1 == Length2 , BoolAns == true ->
                % io:format("Parent of  ~w : ~w ~n",[My_ID,Parent]),
                % io:format("exited normally ~w ~n",[My_ID]);
                exit(normal);
            true ->
                done
            end,
            io:format("Children Set of ~w  :  ~p ~n", [My_ID,Children]),
            % io:format("Unrelated Set of ~w  :  ~p ~n", [My_ID,Unrelated1]),
            received(Parent,My_ID,Neighbours,Children,Unrelated1);  

 		{queryy,From,To,Source_VertexID} ->
 			io:format("QUERY Msg Received to ~w  from  ~w ~n", [To,From]),
            % io:format("Previous parent of ~w : ~w ~n",[My_ID,Parent]),
            % io:format("queryy Received from ~w  to ~w ~n", [From,To]),
            J = Source_VertexID,
            J_set = ordsets:from_list([Source_VertexID]),
            if Parent == -1 -> 
                Parent1 = Source_VertexID,
                % io:format("~w -> ~w ~n",[To,list_to_atom(string:concat("v",integer_to_list(J)))]),
                send(accept,To,list_to_atom(string:concat("v",integer_to_list(J))),My_ID),
                io:format("************* Parent of  ~w : ~w ************** ~n",[list_to_atom(string:concat("v",integer_to_list(My_ID))),list_to_atom(string:concat("v",integer_to_list(Parent1)))]),
                Send_List = ordsets:subtract(Neighbours,J_set),
                Send_List1 = [list_to_atom(string:concat("v",integer_to_list(VID))) || VID <- Send_List],
                sendToList(queryy,To,Send_List1,My_ID),
                Parent_set = ordsets:from_list([Parent1]),
                Children_and_Unrelated = ordsets:union(Children,Unrelated),
                Parent_less_Neighbours = ordsets:subtract(Neighbours,Parent_set),
                % io:format("Children_and_Unrelated ~w : ~p ~n",[My_ID,Children_and_Unrelated]),
                % io:format("Parent_less_Neighbours ~w : ~p ~n",[My_ID,Parent_less_Neighbours]),
                Length1 = ordsets:size(Children_and_Unrelated),
                Length2 = ordsets:size(Parent_less_Neighbours),
                BoolAns = ordsets:is_subset(Children_and_Unrelated,Parent_less_Neighbours),
                % io:format("L1: ~w  ,L2 :~w  ,BoolAns : ~w ~n", [Length1,Length2,BoolAns]),   
                % io:format("vertex : ~w  L1: ~w  ,L2 :~w ~n", [My_ID,Length1,Length2]), 
                if Length1 == Length2 , BoolAns == true ->
                    % io:format("Parent of  ~w : ~w ~n",[My_ID,Parent1]),
                    % io:format("exited normally ~w ~n",[My_ID]);
                    exit(normal);
                true ->
                    done
                end,  
                io:format("Children Set of ~w  :  ~p ~n", [My_ID,Children]),
                % io:format("Unrelated Set of ~w  :  ~p ~n", [My_ID,Unrelated]),
                % io:format("New Parent of ~w  :  ~w ~n", [My_ID,Parent1]),
                received(Parent1,My_ID,Neighbours,Children,Unrelated);   
            true ->
                % io:format("REJECTED Msg Received to ~w from ~w ~n", [To,J]),   
                send(reject,To,list_to_atom(string:concat("v",integer_to_list(J))),My_ID)
            end
    end.
    

bfs(G,Initiater,VertexID) ->

    PID=self(),

    register(list_to_atom(string:concat("v",integer_to_list(VertexID))),PID),
    io:format("Register ~w  :  ~w ~n", [PID,list_to_atom(string:concat("v",integer_to_list(VertexID)))]),

    Parent = -1,

    Unrelated = ordsets:new(),
    Children = ordsets:new(),
    Neighbours = ordsets:from_list(out_neighbours(G,VertexID)),

     % io:format("inside bfs----------~w ~w ~w ~n", [VertexID,Initiater,Parent]),

    if VertexID == Initiater , Parent == -1 -> 
        Neighbours_list = ordsets:to_list(Neighbours), 
        % io:format(" ~w ~p ~n", [VertexID, Neighbours_list]),
        Neighbours_list1 = [list_to_atom(string:concat("v",integer_to_list(VID))) || VID <- Neighbours_list], 
        % io:format(" ~w ~p ~n", [VertexID, Neighbours_list1]),
        sendToList(queryy,list_to_atom(string:concat("v",integer_to_list(VertexID))),Neighbours_list1,VertexID),
        % send(queryy,list_to_atom(string:concat("v",integer_to_list(VertexID))),list_to_atom(string:concat("v",integer_to_list(VertexID))),VertexID);
        Parent1 = VertexID,
        % io:format("****************************************** ~n",[]),
        io:format("************* Parent of  ~w : ~w ************** ~n",[list_to_atom(string:concat("v",integer_to_list(VertexID))),list_to_atom(string:concat("v",integer_to_list(Parent1)))]),
        % io:format("****************************************** ~n",[]),
        received(Parent1,VertexID,Neighbours,Children,Unrelated);
    true->
        % io:format("false bfs---------~n", []),
        received(Parent,VertexID,Neighbours,Children,Unrelated),
        done
    end.

start() ->
    G = digraph:new(),
   
    add_vertex(G,1),
    add_vertex(G,2),
    add_vertex(G,3),
    add_vertex(G,4),
    add_vertex(G,5),

    % vertices(G).
    add_edge(G,1,2),
    add_edge(G,1,3),
    add_edge(G,2,3),
    add_edge(G,2,4),
    % add_edge(G,2,5),
    add_edge(G,3,4),
    add_edge(G,4,5),
    
    % io:format("~p~n", [out_neighbours(G, 1)]),
    % io:format("~p~n", [out_neighbours(G, 2)]),
    % io:format("~p~n", [out_neighbours(G, 3)]),
    % io:format("~p~n", [out_neighbours(G, 4)]),
    % io:format("~p~n", [out_neighbours(G, 5)]),

    spawn(my,bfs,[G,1,1]),
    spawn(my,bfs,[G,-1,2]),
    spawn(my,bfs,[G,-1,3]),
    spawn(my,bfs,[G,-1,4]),
    spawn(my,bfs,[G,-1,5]).