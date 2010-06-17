%%%%  Copyright (c) 2010 Mark Fine <mark.fine@gmail.com>
%%%%
%%%%  Permission is hereby granted, free of charge, to any person
%%%%  obtaining a copy of this software and associated documentation
%%%%  files (the "Software"), to deal in the Software without
%%%%  restriction, including without limitation the rights to use,
%%%%  copy, modify, merge, publish, distribute, sublicense, and/or sell
%%%%  copies of the Software, and to permit persons to whom the
%%%%  Software is furnished to do so, subject to the following
%%%%  conditions:
%%%%
%%%%  The above copyright notice and this permission notice shall be
%%%%  included in all copies or substantial portions of the Software.
%%%%
%%%%  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%%%%  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
%%%%  OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
%%%%  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
%%%%  HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
%%%%  WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%%%%  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
%%%%  OTHER DEALINGS IN THE SOFTWARE.

-module(aws_query, [AWS_ACCESS_KEY, AWS_SECRET_KEY]).

-export([request/4]).

request(Method, Url, Params, ResponseFun) ->
    Timestamp = timestamp(),
    SignParams = Params ++ 
        [{"AWSAccessKeyId", AWS_ACCESS_KEY},
         {"SignatureMethod", "HmacSHA1"},
         {"SignatureVersion", "2"},
         {"Timestamp", Timestamp}],
    MethodStr = string:to_upper(atom_to_list(Method)),
    Host = host(Url),
    Path = path(Url),
    QueryParams = query_params(SignParams),
    SignStr = string:join([MethodStr, Host, Path, QueryParams], [$\n]),
    Signature = sign(AWS_SECRET_KEY, SignStr),
    RequestParams = SignParams ++ 
        [{"Signature", Signature}],
    Request = request_param(Method, Url, RequestParams, []),
    case http:request(Method, Request, [], []) of
        {ok, Response} ->
            ResponseFun(Response);
        {error, Reason} ->
            {error, Reason}
    end.

request_param(get, Url, Params, Headers) ->
    QueryParams = query_params(Params),
    {string:join([Url | QueryParams], [$?]), Headers};
request_param(post, Url, Params, Headers) ->
    QueryParams = query_params(Params),
    {Url, Headers, "application/x-www-form-urlencoded", QueryParams}.

sign(SecretKey, SignStr) ->
    binary_to_list(base64:encode(crypto:sha_mac(SecretKey, SignStr))).

timestamp() ->
    {{Year, Month, Day}, {Hour, Min, Sec}} = erlang:universaltime(),
    lists:flatten(io_lib:format("~4.10.0B-~2.10.0B-~2.10.0BT~2.10.0B:~2.10.0B:~2.10.0BZ",
                                [Year, Month, Day, Hour, Min, Sec])).

host(Url) ->
    Re = "(http://|https://|)([^/:]+)[/:]?.*",
    {match, _} = re:run(Url, Re),
    re:replace(Url, Re, "\\2", [{return, list}]).

path(Url) ->
    Re = "(http://|https://|)[^/]+/?(.*)",
    {match, _} = re:run(Url, Re),
    re:replace(Url, Re, "/\\2", [{return, list}]).

query_params(Params) ->
    string:join([url_encode(Name) ++ [$= | url_encode(Value)] || 
                    {Name, Value} <- lists:sort(Params)], [$&]).

url_encode([H | T]) when H >= $0, H =< $9 ->
    [H | url_encode(T)];
url_encode([H | T]) when H >= $a, H =< $z ->
    [H | url_encode(T)];
url_encode([H | T]) when H >= $A, H =< $Z ->
    [H | url_encode(T)];
url_encode([H | T]) when H == $-; H == $_; H == $. ->
    [H | url_encode(T)];
url_encode([H | T]) ->
    case erlang:integer_to_list(H, 16) of
        [X, Y] ->
            [$%, X, Y | url_encode(T)];
        [X] ->
            [$%, $0, X | url_encode(T)]
    end;
url_encode(_) ->
    [].

