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
-module(pipeline).

%%
%% Include files
%%
-include("private.hrl").
%%
%% Exported Functions
%%

-export([new_filter/3, 
		 new_mapper/3, 
		 new_folder/4, 
		 new_mff/4,
		 new_switch/4,
		 new_evt/3,
		 new_gate_src/4,
		 new_gate_dst/4,
		 connect/3,
		 interconnect/4,
		 whereis/2,
		 pushf/2]).

-type filter_fun(V) 	 :: fun( (V) -> boolean() ).
-type map_fun(V1,V2)  	 :: fun( (V1) -> V2 ).
-type fold_fun(V, Acc) :: fun((V, Acc)-> Acc).
-type mff_fun(V, Acc)	 :: fun((V, Acc) -> {out, V1 :: any(), Acc}) | {nout, Acc}.
-type switch_fun(V, Acc) :: fun((V, Acc)-> {PipeName :: pipe_name(), Acc}).

-type pipe_name()		 :: any().
-type pipeline_ref()	 :: Name :: atom() 								| 
		  					{Name :: atom(), Node :: node()} 			| 
		  					{global, GlobalName :: any()} 				| 
		  					{via, Module :: atom(), ViaName :: atom()} 	| 
		  					pid().
-type pipe_ref() 		 :: {pipeline_ref(), pipe_name()}.

-export_type([filter_fun/1, 
			  map_fun/2, 
			  fold_fun/2, 
			  mff_fun/2]).

%%
%% API Functions
%%

%----------------------------------------------
%
%----------------------------------------------
-spec new_filter(Pipeline :: pipeline_ref(), 
				 PipeName :: pipe_name(), 
				 PredFun  :: filter_fun( any() ) ) -> pipe_ref().
new_filter(Pipeline, PipeName, PredFun) ->
	PipeSpec = #pipe_spec{pipeline=Pipeline, 
						  name=PipeName, 
						  pipe_type=filter, 
						  args=PredFun},
	pipeline_ctrl_srv:create_pipe(Pipeline, PipeSpec).
%----------------------------------------------
%
%----------------------------------------------
-spec new_mapper(Pipeline :: pipeline_ref(), 
				 PipeName :: pipe_name(), 
				 MapFun   :: map_fun( any(), any() ) ) -> pipe_ref().
new_mapper(Pipeline, PipeName, MapFun) ->
	PipeSpec = #pipe_spec{pipeline=Pipeline, 
						  name=PipeName, 
						  pipe_type=map, 
						  args=MapFun},
	pipeline_ctrl_srv:create_pipe(Pipeline, PipeSpec).
%----------------------------------------------
%
%----------------------------------------------
-spec new_folder(Pipeline :: pipeline_ref(),
				 PipeName :: pipe_name(),
				 FoldFun  :: fold_fun( any(), any() ),
				 Acc0	  :: any() ) -> pipe_ref().
new_folder(Pipeline, PipeName, FoldFun, Acc0) ->
	PipeSpec = #pipe_spec{pipeline=Pipeline, 
						  name=PipeName, 
						  pipe_type=fold, 
						  args=[FoldFun, Acc0]},
	pipeline_ctrl_srv:create_pipe(Pipeline, PipeSpec).
%----------------------------------------------
%
%----------------------------------------------
-spec new_mff(Pipeline :: pipeline_ref(),
			  PipeName :: pipe_name(),
			  MffFun   :: mff_fun( any(), any() ),
			  Acc0 	   :: any() ) -> pipe_ref().
new_mff(Pipeline, PipeName, MffFun, Acc0) ->
	PipeSpec = #pipe_spec{pipeline=Pipeline, 
						  name=PipeName, 
						  pipe_type=mff, 
						  args=[MffFun, Acc0]},
	pipeline_ctrl_srv:create_pipe(Pipeline, PipeSpec).
%----------------------------------------------
%
%----------------------------------------------
-spec new_switch(Pipeline :: pipeline_ref(),
			  PipeName 	  :: pipe_name(),
			  SwitchFun   :: switch_fun( any(), any() ),
			  Acc0 	   	  :: any() ) -> pipe_ref().
new_switch(Pipeline, PipeName, SwitchFun, Acc0) ->
	PipeSpec = #pipe_spec{pipeline=Pipeline, 
						  name=PipeName, 
						  pipe_type=switch, 
						  args=[SwitchFun, Acc0]},
	pipeline_ctrl_srv:create_pipe(Pipeline, PipeSpec).
%----------------------------------------------
%
%----------------------------------------------
-spec new_evt(	Pipeline :: pipeline_ref(),
		 		PipeName :: pipe_name(),
		 		EventManagerRef  :: term() ) -> ok.
new_evt(Pipeline, PipeName, EventManagerRef) ->
	ok.
%----------------------------------------------
%
%----------------------------------------------
-spec new_gate_dst(Pipeline 	 :: pipeline_ref(),
				   PipeName 	 :: pipe_name(),
				   PipelineDst 	 :: pipeline_ref(),
				   PipeNameDst 	 :: pipe_name()) -> ok.
new_gate_dst(Pipeline, PipeName, PipelineDst, PipeNameDst) ->
	PipeSpec = #pipe_spec{pipeline=Pipeline, 
						  name=PipeName,
						  pipe_type={gate,dst}, 
						  args=[PipelineDst, PipeNameDst]},
	pipeline_ctrl_srv:create_pipe(Pipeline, PipeSpec).
%----------------------------------------------
%
%----------------------------------------------
-spec new_gate_src(Pipeline 	 :: pipeline_ref(),
				   PipeName 	 :: pipe_name(),
				   PipelineDst 	 :: pipeline_ref(),
				   PipeNameDst 	 :: pipe_name()) -> ok.
new_gate_src(Pipeline, PipeName, PipelineSrc, PipeNameSrc) ->
	PipeSpec = #pipe_spec{pipeline=Pipeline, 
						  name=PipeName,
						  pipe_type={gate,src}, 
						  args=[PipelineSrc, PipeNameSrc]},
	pipeline_ctrl_srv:create_pipe(Pipeline, PipeSpec).
%----------------------------------------------
%
%----------------------------------------------
-spec interconnect(PipelineSrc :: pipeline_ref(), 
				   PipeNameSrc :: pipe_name(), 
				   PipelineDst :: pipeline_ref(), 
				   PipeNameDst :: pipe_name()) -> ok.
interconnect(PipelineSrc, PipeNameSrc, PipelineDst, PipeNameDst) ->
	ok.

%----------------------------------------------
%
%----------------------------------------------
-spec connect(Pipeline		:: pipeline_ref(), 
			  PipeSrcName 	:: pipe_name(), 
			  PipeDestName 	:: pipe_name()) -> ok.
connect(Pipeline, PipeSrcName, PipeDestName) ->
	pipeline_ctrl_srv:connect_pipes(Pipeline, PipeSrcName, PipeDestName).
%----------------------------------------------
%
%----------------------------------------------
-spec connect_all(Pipeline		:: pipeline_ref(),
			  	  PipeNames 	:: [pipe_name()]) -> ok.
connect_all(Pipeline, PipeNames) ->
	ok.
%----------------------------------------------
%
%----------------------------------------------
-spec whereis(Pipeline		:: pipeline_ref(), 
			  PipeName 		:: pipe_name() ) -> pid() | undefined.
whereis(Pipeline, PipeName) ->
	try	pipeline_ctrl_srv:get_pipe_pid(Pipeline, PipeName)
	catch 
		_:{noproc, _} 		-> undefined;
		_:{{nodedown, _},_} -> undefined 
	end.

pushf(Pipeline, PipeName) ->
	case whereis(Pipeline, PipeName) of
		undefined -> undefined;
		PipePid   -> fun(Val) ->
							 gen_server:cast(PipePid, {push, Val})
					 end
	end.
			
%----------------------------------------------
%
%----------------------------------------------
-spec info(Pipeline :: pipeline_ref()) -> ok.
info(Pipeline) ->
	ok.
	

%%
%% Local Functions
%%

