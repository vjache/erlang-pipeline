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
-module(pipeline_sup).

-behaviour(supervisor).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("private.hrl").
%% --------------------------------------------------------------------
%% External exports
%% --------------------------------------------------------------------

-export([start_link/0, start_link/1, start_pipe/2]).

-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(SUPER(I, Args),     {I, {?MODULE, start_link, Args}, permanent, 5000, supervisor, [?MODULE]}).
-define(CHILD(I, Args), 	{I, {I, start_link, Args}, permanent, 5000, worker, [I]}).
-define(CHILD_EVT_MAN(I),   {I, {I, start_link, []}, permanent, 5000, worker, dynamic}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
	supervisor:start_link(?MODULE, root).

start_link({instance, PipelineModule}) when is_atom(PipelineModule) ->
    supervisor:start_link(?MODULE, {instance, PipelineModule});
start_link({instance_pipes, PipesSupRegName, PipelineModule}) ->
    case supervisor:start_link({local, PipesSupRegName}, ?MODULE, []) of
		{ok, _Pid} = Ret -> PipelineModule:construct(), Ret;
		Other 			 -> Other
	end.

start_pipe(Pipeline, #pipe_spec{name=PipeLabel}=PipeSpec) ->
	ChSpec = {PipeLabel ,
			  {gen_server, start_link, [pipeline_pipe_srv, PipeSpec, [] ]},
			  permanent,
			  2000,
			  worker,
			  [pipeline_pipe_srv]},
	case supervisor:start_child(Pipeline, ChSpec) of
		{ok, Child} 		-> Child;
		{ok, Child, _Info} 	-> Child;
		{error, Reason} 	-> throw(Reason)
	end.

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================
init(root) ->
	PipelineModule=pipeline_test,
	{ok, { {one_for_one, 5, 10}, 
		   [?SUPER(PipelineModule, [{instance, PipelineModule}])] } };
init({instance, PipelineModule}) ->
	PipesSupRegName=list_to_atom(atom_to_list(PipelineModule) ++ ".pipes_sup"),
	{ok, { {rest_for_one, 5, 10}, 
		   [?CHILD(pipeline_ctrl_srv, [PipesSupRegName, PipelineModule]),
			?SUPER(pipes, 			  [{instance_pipes, PipesSupRegName, PipelineModule}])] } };
init([]) ->
	{ok, { {one_for_one, 5, 10}, 
		   [] } }.
