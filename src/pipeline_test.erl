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
%%% Created : Sep 28, 2012
%%%-------------------------------------------------------------------------------
-module(pipeline_test).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([get_name/0, construct/0]).

%%
%% API Functions
%%

-define(PL, {local, ?MODULE}).

get_name() -> ?PL.

construct() ->
	pipeline:new_filter(
	  ?PL, f1, fun(N) -> (N rem 2) == 0 end),
	construct_switch(sw),
	pipeline:connect(?PL, f1, sw),
	case node() of
		'n0@hydra' ->
			pipeline:new_gate_src(?PL, src1, {pipeline_test, 'n1@hydra'}, mpr1),
			construct_printer(prn_src1, 1),
			pipeline:connect(?PL, src1, prn_src1);
		'n1@hydra' ->
			pipeline:new_mapper(
	  ?PL, mpr1, fun(N) -> N end)
	end.

construct_switch(SwithPipeName) ->
	BNum=10,
	PrnPipeNameBase = prn,
	SwitchFun=fun(Val, _Acc) -> 
					  {{PrnPipeNameBase, erlang:phash2(Val, BNum)}, _Acc}
			  end,
	pipeline:new_switch(
	  ?PL, SwithPipeName, 
	  SwitchFun, []),
	[begin 
		 PrnPipe={PrnPipeNameBase, N},
		 construct_printer(PrnPipe, 1000),
		 pipeline:connect(?PL, SwithPipeName, PrnPipe)
	 end || N <- lists:seq(0, BNum-1)].

construct_printer(PrnPipeName, Thr) ->
	pipeline:new_mapper(
	  ?PL, PrnPipeName, 
	  fun(N) -> if (N rem Thr)==0 -> io:format("~p>>~p~n",[PrnPipeName, N]);
				   true -> ok
				end
	  end).

%%
%% Local Functions
%%

