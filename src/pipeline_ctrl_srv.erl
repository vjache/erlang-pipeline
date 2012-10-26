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
%%% Created : Sep 27, 2012
%%%-------------------------------------------------------------------------------
-module(pipeline_ctrl_srv).

-behaviour(gen_server).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("private.hrl").
%% --------------------------------------------------------------------
%% External exports
-export([start_link/2, create_pipe/2]).

%% gen_server callbacks
-export([init/1, 
		 handle_call/3, 
		 handle_cast/2, 
		 handle_info/2, 
		 terminate/2, 
		 code_change/3,
		 connect_pipes/3,
		 register_pipe_pid/3,
		 get_pipe_pid/2,
		 get_pipeline_name/1]).

-record(state, {pipeline_module, sup, graph}).

%% ====================================================================
%% External functions
%% ====================================================================

%% ====================================================================
%% Server functions
%% ====================================================================

start_link(SupRef, PipelineModule) ->
	PipelineName=PipelineModule:get_name(),
	gen_server:start_link(
	  PipelineName, 
	  ?MODULE, [SupRef, PipelineModule], []).

create_pipe(Pipeline, PipeSpec) ->
	throw_error(gen_server:call(to_srv_ref(Pipeline), {create_pipe, PipeSpec})).

connect_pipes(Pipeline, PipeNameSrc, PipeNameDst) ->
	throw_error(gen_server:call(to_srv_ref(Pipeline), {connect_pipes, PipeNameSrc, PipeNameDst})).

register_pipe_pid(Pipeline, PipeName, PipePid) ->
	throw_error(gen_server:cast(to_srv_ref(Pipeline), {register_pipe_pid, {PipeName, PipePid}})).

get_pipe_pid(Pipeline, PipeName) ->
	throw_error(gen_server:call(to_srv_ref(Pipeline), {get_pipe_pid, PipeName})).

get_pipeline_name(CtrlPid) ->
	throw_error(gen_server:call(to_srv_ref(CtrlPid), get_pipeline_name)).

%% --------------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State}          |
%%          {ok, State, Timeout} |
%%          ignore               |
%%          {stop, Reason}
%% --------------------------------------------------------------------
init([SupRef, PipelineModule]) ->
	G=digraph:new([acyclic, protected]),
    {ok, #state{pipeline_module=PipelineModule, sup=SupRef, graph=G}}.

%% --------------------------------------------------------------------
%% Function: handle_call/3
%% Description: Handling call messages
%% Returns: {reply, Reply, State}          |
%%          {reply, Reply, State, Timeout} |
%%          {noreply, State}               |
%%          {noreply, State, Timeout}      |
%%          {stop, Reason, Reply, State}   | (terminate/2 is called)
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_call({create_pipe, #pipe_spec{name=PipeName}=PipeSpec}, _From, 
			#state{sup=SupRef, pipeline_module=PMod, graph=G}=State) ->
	case digraph:vertex(G, PipeName) of
		false ->
			PipeSpec1=PipeSpec#pipe_spec{pipeline=PMod:get_name()},
			PipePid=pipeline_sup:start_pipe(SupRef, PipeSpec1),
			digraph:add_vertex(G, PipeName, PipePid),
			{reply, {PMod:get_name(), PipeName}, State};
		_ -> 
			{reply, {error,{pipe_name_in_use, PipeName}}, State}
	end;
handle_call({connect_pipes, PipeNameSrc, PipeNameDst}, _From, 
			#state{graph=G}=State) ->
	try
		{_, PipePidSrc} = get_pipe_vertex(G, PipeNameSrc),
		PDist = {_, _} =  get_pipe_vertex(G, PipeNameDst),
		Edge={PipeNameSrc, PipeNameDst},
		Edge=digraph:add_edge(G, Edge, PipeNameSrc, PipeNameDst, ""),
		pipeline_pipe_srv:add_nexta(PipePidSrc, PDist),
		{reply, ok, State}
	catch 
		_:Reason -> 
			{reply, {error , Reason}, State}
	end;
handle_call({get_pipe_pid, PipeName}, _From, 
			#state{graph=G}=State) ->
	{reply, 
	 case digraph:vertex(G, PipeName) of
		 {_, PipePid} -> PipePid;
		 false 		  -> undefined
	 end, State};
handle_call(get_pipeline_name, _From, #state{pipeline_module=PMod}=State) ->
    {reply, PMod:get_name(), State};
handle_call(_Request, _From, State) ->
    {reply, unexpected, State}.

get_pipe_vertex(G, PipeName) ->
	case digraph:vertex(G, PipeName) of
		false  -> throw({unknown_pipe, PipeName});
		Vertex -> Vertex
	end.
%% --------------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_cast({register_pipe_pid, {PipeName, PipePid}=Pipe}, 
			#state{graph=G}=State) ->
	% Update PID for pipe
	case digraph:vertex(G, PipeName) of
		{_, PipePid} -> ok;
		{_, OldPipePid} ->
			is_pid(OldPipePid) andalso 
				is_process_alive(OldPipePid) andalso 
					throw({pipe_alive, OldPipePid}),
			digraph:add_vertex(G, PipeName, PipePid)
	end,
	% Add destination pipes PIDs
	[begin
		 PDst={_,_}=digraph:vertex(G, DstPipe), 
		 pipeline_pipe_srv:add_nexta(PipePid, PDst)
	 end || DstPipe <- digraph:out_neighbours(G, PipeName)],
	% Add PID to source pipes
	[begin
		 {_,SrcPipePid}=digraph:vertex(G, SrcPipe), 
		 pipeline_pipe_srv:add_nexta(SrcPipePid, Pipe)
	 end || SrcPipe <- digraph:in_neighbours(G, PipeName)],
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

%% --------------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%% --------------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%% --------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%% --------------------------------------------------------------------
%% Func: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%% --------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% --------------------------------------------------------------------
%%% Internal functions
%% --------------------------------------------------------------------


throw_error({error, Reason}) ->
	throw(Reason);
throw_error(NotErr) ->
	NotErr.

to_srv_ref({local, Name}) when is_atom(Name) ->
	Name;
to_srv_ref({global, _Name}=Ref) ->
  Ref;
to_srv_ref({Name, Node}=Ref) when is_atom(Name),is_atom(Node) ->
	Ref;
to_srv_ref(Pid) when is_pid(Pid) ->
	Pid.
