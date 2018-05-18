-module(bFS).
-import(digraph,[graph/0,add_edge/3,add_vertex/2,vertices/1,out_neighbours/2]).
-import(ordsets,[add_element/2]).
-export([start/0,spawner/5,send/4, received/5,bfs/3,sendToList/4,string_to_list/1,string_to_list1/1,add_vertices/2,add_edge1/5,add_edge2/3]).

string_to_list(String) ->
    lists:map(fun(X) -> {Int, _} = string:to_integer(X), 
                        Int end, 
              L=string:tokens(String,"\n")),
                L.

string_to_list1(String) ->
    lists:map(fun(X) -> {Int, _} = string:to_integer(X), 
                        Int end, 
              L=string:tokens(String,",")),
                L.

add_edge2(G,_,[]) ->
	G;

add_edge2(G,Vertex,Neighbours) -> 
    [H|T]=Neighbours,
    % io:fwrite("~w ~n",[H]),
    {Vertex2, _} = string:to_integer(H),
    % io:fwrite("~w ~n",[Vertex2]),
    add_edge(G,Vertex,Vertex2),
    add_edge2(G,Vertex,T),
    G.

add_edge1(G,Vertex,Index,L,V) when Index == V+3  ->
	G;

add_edge1(G,Vertex,Index,L,V) ->
	Neighbours_string = lists:nth(Index,L),
    if Neighbours_string == " " ->
        done;
    true ->
    	% io:fwrite("~p ~n",[Neighbours_string]),
    	Neighbours = string_to_list1(Neighbours_string),

    	% Neighbours = [ string:to_integer(X) || X <- Neighbours1],
        % io:fwrite("~p ~n~n",[Neighbours]),
        G = add_edge2(G,Vertex,Neighbours)
    end,
    G = add_edge1(G,Vertex+1,Index+1,L,V),
	G.

add_vertices(G,[]) ->
	G;

add_vertices(G,Vertices) ->
	[H|T] = Vertices,
	add_vertex(G,H),
	add_vertices(G,T),
	G.

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
            % io:format("Children Set of ~w  :  ~p ~n", [My_ID,Children1]),
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
            % io:format("Children Set of ~w  :  ~p ~n", [My_ID,Children]),
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
                % io:format("Children Set of ~w  :  ~p ~n", [My_ID,Children]),
                % io:format("Unrelated Set of ~w  :  ~p ~n", [My_ID,Unrelated]),
                % io:format("New Parent of ~w  :  ~w ~n", [My_ID,Parent1]),
                received(Parent1,My_ID,Neighbours,Children,Unrelated);   
            true ->
                % io:format("REJECTED Msg Received to ~w from ~w ~n", [To,J]),   
                send(reject,To,list_to_atom(string:concat("v",integer_to_list(J))),My_ID)
            end
    end.
    
spawner(_,_,_,[],_) ->
    done;

spawner(Module,Fun,G,Vertices,Initiator) ->
    [H|T]=Vertices,
    spawn(Module,Fun,[G,Initiator,H]),
    spawner(Module,Fun,G,T,Initiator).

bfs(G,Initiator,VertexID) ->

    PID=self(),

    register(list_to_atom(string:concat("v",integer_to_list(VertexID))),PID),
    io:format("Register ~w  :  ~w ~n", [PID,list_to_atom(string:concat("v",integer_to_list(VertexID)))]),

    Parent = -1,

    Unrelated = ordsets:new(),
    Children = ordsets:new(),
    Neighbours = ordsets:from_list(out_neighbours(G,VertexID)),

     % io:format("inside bfs----------~w ~w ~w ~n", [VertexID,Initiator,Parent]),

    if VertexID == Initiator , Parent == -1 -> 
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
   
    {ok, File} = file:open("./input.txt",[read]),
    {ok, Txt} = file:read(File,1024 * 1024), 
    L = string_to_list(Txt),
    % io:fwrite("~p~n",[L]),

    G = digraph:new(),
    {V,[]} = string:to_integer(lists:nth(1,L)),
    {Initiator,[]} = string:to_integer(lists:nth(2,L)),
    Vertices = lists:seq(1,V),
   	G = add_vertices(G,Vertices), 
    G = add_edge1(G,1,3,L,V),

    % io:format("~p~n", [out_neighbours(G, 1)]),
    % io:format("~p~n", [out_neighbours(G, 2)]),
    % io:format("~p~n", [out_neighbours(G, 3)]),
    % io:format("~p~n", [out_neighbours(G, 4)]),
    % io:format("~p~n", [out_neighbours(G, 5)]),

    spawner(bFS,bfs,G,Vertices,Initiator).