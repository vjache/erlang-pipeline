%%%-------------------------------------------------------------------
%%% @author <vjache@gmail.com>
%%% @copyright (C) 2011, Vyacheslav Vorobyov.  All Rights Reserved.
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%% http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%
%%% @doc
%%%     TODO: Doc need
%%% @end
%%% Created : Sep 25, 2012
%%%-------------------------------------------------------------------------------
-module(pipeline_pipe_srv).

-behaviour(gen_server).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("private.hrl").
%% --------------------------------------------------------------------
%% External exports
-export([pusha/2, 
		 add_nexta/2,
		 get_name/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(pipe_state, {pipeline, name, type, func, acc, next_pipes = [] :: [pid()], name2pipe = dict:new()}).

%% ====================================================================
%% External functions
%% ====================================================================

pusha(Pipe, Val) ->
	gen_server:cast(Pipe, {push, Val}).

add_nexta(Pipe, NextPipe) when tuple_size(NextPipe) == 2 ->
	gen_server:cast(Pipe, {add_next_pipe, NextPipe}).

get_name(Pipe) ->
	gen_server:call(Pipe, get_name).
%% ====================================================================
%% Server functions
%% ====================================================================

%% INIT SECTION
init(#pipe_spec{pipeline=Pipeline, name=Name, pipe_type=T, args = Args}=Spec) ->
	register_pipe_srv(Spec),
	case T of
		filter 	 	-> Fun=Args, Acc=undefined;
		map    	 	-> Fun=Args, Acc=undefined;
		foreach	 	-> Fun=Args, Acc=undefined;
		fold   	 	-> [Fun, Acc]=Args;
		mff	   	 	-> [Fun, Acc]=Args;
		switch 		-> [Fun, Acc]=Args;
		{gate,_} 	-> [PipelineRemote, PipeNameRemote]=Args,
					   Fun=fun() -> 
%% 								   io:format("~p~n", [{'pipeline:whereis', [PipelineRemote, PipeNameRemote]}]),
								   pipeline:whereis(PipelineRemote, PipeNameRemote) 
						   end,
					   Acc=undefined,
					   self() ! locate_remote_pipe
%% 	;evt		 -> ok
	end,		
	{ok, #pipe_state{pipeline=Pipeline, name=Name, type=T, func = Fun, acc = Acc}}.

register_pipe_srv(#pipe_spec{pipeline=Pipeline,name=PipeName}) ->
	pipeline_ctrl_srv:register_pipe_pid(Pipeline, PipeName, self()).

%% HANDLE CALL SECTION
handle_call(get_name, _From, #pipe_state{name=Name}=State) ->
    {reply, Name, State};
handle_call(_Request, _From, State) ->
    {reply, unexpected, State}.

%% HANDLE CAST SECTION
handle_cast({push, Value}, 
			#pipe_state{type=mff, func=MFFFun, acc=Acc0}=State) ->
	case MFFFun(Value, Acc0) of
		{out, Value1, Acc1} -> multi_cast_({push, Value1}, State);
		{nout, Acc1} 		-> ok
	end,
	{noreply, State#pipe_state{acc=Acc1}};

handle_cast({push, Value}, 
			#pipe_state{type = fold, func=FoldFun, acc=Acc0}=State) ->
	Acc1 = FoldFun(Value, Acc0), 
	multi_cast_({push, Acc1}, State),
	{noreply, State#pipe_state{acc=Acc1}};

handle_cast({push, Value}=Msg, 
			#pipe_state{type = foreach, func=ActionFun}=State) ->
	ActionFun(Value), 
	multi_cast_(Msg, State),
	{noreply, State};

handle_cast({push, Value}, 
			#pipe_state{type=map, func=MapFun}=State) ->
	Value1 = MapFun(Value), 
	multi_cast_({push, Value1}, State),
	{noreply, State};

handle_cast({push, Value}=Msg, 
			#pipe_state{type=filter, func=PredFun}=State) ->
	case PredFun(Value) of
		true 	-> multi_cast_(Msg, State);
		false 	-> ok
	end,
	{noreply, State};

handle_cast({push, Value}=Msg, 
			#pipe_state{type=switch, func=SwitchFun, name2pipe=N2P, acc=Acc0}=State) ->
	{PipeNameDst, Acc1} = SwitchFun(Value, Acc0),
	case dict:find(PipeNameDst, N2P) of
		{ok, PipePidDst} 	-> gen_server:cast(PipePidDst, Msg);
		error 				-> io:format("NOT_FOUND:~p~n",[PipeNameDst])
	end,
	{noreply, 
	 if Acc0 =:= Acc1 -> State;
		true		  -> State#pipe_state{acc=Acc1}
	 end};

handle_cast({push, _Value}=Msg, 
			#pipe_state{type={gate,_}}=State) ->
	multi_cast_(Msg, State),
	{noreply, State};

handle_cast({add_next_pipe, Pipe}, 
			State) ->
	{noreply, add_next_pipe_(Pipe, State)}.

%% HANDLE INFO SECTION
handle_info({'DOWN', _MRef, process, Object, Reason}, #pipe_state{type={gate,_},acc=Object}=State) ->
    {stop, Reason, State};

handle_info({'DOWN', _MRef, process, Object, _Info}, #pipe_state{}=State) ->
    {noreply, clear_pipe_pid(Object, State)};

handle_info(locate_remote_pipe=Msg, #pipe_state{func=LocateFun}=State) ->
	case LocateFun() of
		undefined -> 
			erlang:send_after(5000, self(), Msg),
			{noreply, State};
		PipePid when is_pid(PipePid) ->
			erlang:monitor(process, PipePid),
			Type=State#pipe_state.type,
			case Type of
				{gate, src} ->
					FqName={node(),Type,self()},
					pipeline_pipe_srv:add_nexta(PipePid, {FqName, self()}),
					{noreply, State#pipe_state{acc=PipePid}};
				{gate, dst} -> 
					FqName={node(PipePid), PipePid},
					pipeline_pipe_srv:add_nexta(self(), {FqName, PipePid}),
					{noreply, State#pipe_state{acc=PipePid}}
			end
	end;

handle_info(_Info, State) ->
    {noreply, State}.

%% HANDLE TERMINATE SECTION
terminate(_Reason, _State) ->
    ok.

%% HANDLE CODE CHANGE SECTION
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% --------------------------------------------------------------------
%%% Internal functions
%% --------------------------------------------------------------------

clear_pipe_pid(PipePid, #pipe_state{next_pipes=Nexts, name2pipe=N2P}=State) ->
	Nexts1=ordsets:del_element(PipePid, Nexts),
	N2P1=dict:fold(
		   fun(N, P, Acc) when P == PipePid->
				   dict:erase(N, Acc);
			  (_, _, Acc) -> Acc
		   end, N2P, N2P),
	State#pipe_state{next_pipes=Nexts1, name2pipe=N2P1}.

multi_cast_(Msg, #pipe_state{next_pipes=Pids}) -> 
	[gen_server:cast(P, Msg) || P <- Pids].

add_next_pipe_({PipeName, PipePid}, #pipe_state{next_pipes=Nexts, name2pipe=N2P}=State) -> 
	case ordsets:is_element(PipePid, Nexts) of
		true -> State;
		false -> 
			erlang:monitor(process, PipePid),
			Nexts1=ordsets:add_element(PipePid, Nexts),
			N2P1=dict:store(PipeName, PipePid, N2P),
			State#pipe_state{next_pipes=Nexts1, name2pipe=N2P1}
	end.

%% basic_test() ->
%% 	P1 = start_link_filter(fun(N) -> N rem 2 == 0 end ),
%% 	P2 = start_link_mapper(fun(N) -> N * N end ),
%% 	P3 = start_link_folder(fun(N, Acc) -> N + Acc end, 0 ),
%% 	P4 = start_link_mff(fun(N, Acc) -> Acc1 = Acc + N, {out, Acc1, Acc1} end, 0 ),
%% 	add_nexta(P1, P2),
%% 	add_nexta(P2, P3),
%% 	add_nexta(P3, P4),
%% 	[push(P1, N) || N <- lists:seq(1, 1000)].