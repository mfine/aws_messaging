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

-module(aws_sns, [AWS_SNS_URL, AWS_ACCESS_KEY, AWS_SECRET_KEY]).

-export([create_topic/1, create_topic_detail/1, create_topic_request/3,
         delete_topic/1, delete_topic_detail/1, delete_topic_request/3,
         list_topics/0, list_topics_detail/0, list_topics_request/2,
         subscribe/3, subscribe_detail/3, subscribe_request/5,
         confirm_subscription/2, confirm_subscription_detail/2, confirm_subscription_request/4,
         unsubscribe/1, unsubscribe_detail/1, unsubscribe_request/3,
         list_subscriptions/0, list_subscriptions_detail/0, list_subscriptions_request/2,
         list_subscriptions_by_topic/1, list_subscriptions_by_topic_detail/1, list_subscriptions_by_topic_request/3,
         publish/2, publish_detail/2, publish/3, publish_detail/3, publish_request/4,
         get_topic_attributes/1, get_topic_attributes_detail/1, get_topic_attributes_request/3,
         set_topic_attributes/3, set_topic_attributes_detail/3, set_topic_attributes_request/5,
         add_permission/4, add_permission_detail/4, add_permission_request/4,
         remove_permission/2, remove_permission_detail/2, remove_permission_request/4]).

-define(AWS_QUERY, (aws_query:new(AWS_ACCESS_KEY, AWS_SECRET_KEY))).

%% Create Topic 

create_topic(Name) ->
    create_topic_request(Name, [], fun create_topic_response/3).

create_topic_detail(Name) ->
    create_topic_request(Name, [], fun create_topic_response_detail/3).

create_topic_request(Name, Params, Response) ->
    RequestParams = Params ++
        [{"Name", Name},
         {"Action", "CreateTopic"}],
    request(post, AWS_SNS_URL, RequestParams, Response).

create_topic_response(_Headers, _RequestId, Doc) ->
    TopicArn = aws_xpath:value("//TopicArn", Doc),
    {ok, TopicArn}.

create_topic_response_detail(_Headers, RequestId, Doc) ->
    TopicArn = aws_xpath:value("//TopicArn", Doc),
    {ok, [RequestId, {"TopicArn", TopicArn}]}.


%% Delete Topic 

delete_topic(TopicArn) ->
    delete_topic_request(TopicArn, [], fun void_response/3).

delete_topic_detail(TopicArn) ->
    delete_topic_request(TopicArn, [], fun void_response_detail/3).

delete_topic_request(TopicArn, Params, Response) ->
    RequestParams = Params ++
        [{"TopicArn", TopicArn},
         {"Action", "DeleteTopic"}],
    request(post, AWS_SNS_URL, RequestParams, Response).

%% List Topics

list_topics() ->
    list_topics_request([], fun list_topics_response/3).

list_topics_detail() ->
    list_topics_request([], fun list_topics_response_detail/3).

list_topics_request(Params, Response) ->
    RequestParams = Params ++
        [{"Action", "ListTopics"}],
    request(post, AWS_SNS_URL, RequestParams, Response).

list_topics_response(_Headers, _RequestId, Doc) ->
    TopicArns = [aws_xpath:value("./TopicArn", Member) ||
                    Member <- aws_xpath:nodes("//member", Doc)],
    {ok, TopicArns}.

list_topics_response_detail(_Headers, RequestId, Doc) ->
    TopicArns = [{"TopicArn", aws_xpath:value("./TopicArn", Member)} ||
                    Member <- aws_xpath:nodes("//member", Doc)],
    {ok, [RequestId, {"TopicArns", TopicArns}]}.

%% Subscribe

subscribe(TopicArn, Protocol, Endpoint) ->
    subscribe_request(TopicArn, Protocol, Endpoint, [], fun subscribe_response/3).

subscribe_detail(TopicArn, Protocol, Endpoint) ->
    subscribe_request(TopicArn, Protocol, Endpoint, [], fun subscribe_response_detail/3).

subscribe_request(TopicArn, Protocol, Endpoint, Params, Response) ->
    RequestParams = Params ++
        [{"TopicArn", TopicArn},
         {"Protocol", Protocol},
         {"Endpoint", Endpoint},
         {"Action", "Subscribe"}],
    request(post, AWS_SNS_URL, RequestParams, Response).

subscribe_response(_Headers, _RequestId, Doc) ->
    SubscriptionArn = aws_xpath:value("//SubscriptionArn", Doc),
    {ok, SubscriptionArn}.

subscribe_response_detail(_Headers, RequestId, Doc) ->
    SubscriptionArn = aws_xpath:value("//SubscriptionArn", Doc),
    {ok, [RequestId, {"SubscriptionArn", SubscriptionArn}]}.

%% Confirm Subscription

confirm_subscription(TopicArn, Token) ->
    confirm_subscription_request(TopicArn, Token, [], fun subscribe_response/3).

confirm_subscription_detail(TopicArn, Token) ->
    confirm_subscription_request(TopicArn, Token, [], fun subscribe_response_detail/3).

confirm_subscription_request(TopicArn, Token, Params, Response) ->
    RequestParams = Params ++
        [{"TopicArn", TopicArn},
         {"Token", Token},
         {"Action", "ConfirmSubscription"}],
    request(post, AWS_SNS_URL, RequestParams, Response).

%% Unsubscribe

unsubscribe(SubscriptionArn) ->
    unsubscribe_request(SubscriptionArn, [], fun void_response/3).

unsubscribe_detail(SubscriptionArn) ->
    unsubscribe_request(SubscriptionArn, [], fun void_response_detail/3).

unsubscribe_request(SubscriptionArn, Params, Response) ->
    RequestParams = Params ++
        [{"SubscriptionArn", SubscriptionArn},
         {"Action", "Unsubscribe"}],
    request(post, AWS_SNS_URL, RequestParams, Response).

%% List Subscriptions

list_subscriptions() ->
    list_subscriptions_request([], fun list_subscriptions_response/3).

list_subscriptions_detail() ->
    list_subscriptions_request([], fun list_subscriptions_response_detail/3).

list_subscriptions_request(Params, Response) ->
    RequestParams = Params ++
        [{"Action", "ListSubscriptions"}],
    request(post, AWS_SNS_URL, RequestParams, Response).

list_subscriptions_response(_Headers, _RequestId, Doc) ->
    SubscriptionArns = [aws_xpath:value("./SubscriptionArn", Member) ||
                           Member <- aws_xpath:nodes("//member", Doc)],
    {ok, SubscriptionArns}.

list_subscriptions_response_detail(_Headers, RequestId, Doc) ->
    SubscriptionArns = [[{"SubscriptionArn", aws_xpath:value("./SubscriptionArn", Member)},
                         {"Owner", aws_xpath:value("./Owner", Member)},
                         {"Protocol", aws_xpath:value("./Protocol", Member)},
                         {"Endpoint", aws_xpath:value("./Endpoint", Member)},
                         {"TopicArn", aws_xpath:value("./TopicArn", Member)}] ||
                           Member <- aws_xpath:nodes("//member", Doc)],
    {ok, [RequestId, {"SubscriptionArns", SubscriptionArns}]}.

%% List Subscriptions By Topic

list_subscriptions_by_topic(TopicArn) ->
    list_subscriptions_by_topic_request(TopicArn, [], fun list_subscriptions_response/3).

list_subscriptions_by_topic_detail(TopicArn) ->
    list_subscriptions_by_topic_request(TopicArn, [], fun list_subscriptions_response_detail/3).

list_subscriptions_by_topic_request(TopicArn, Params, Response) ->
    RequestParams = Params ++
        [{"TopicArn", TopicArn},
         {"Action", "ListSubscriptionsByTopic"}],
    request(post, AWS_SNS_URL, RequestParams, Response).

%% Publish

publish(TopicArn, Message) ->
    publish_request(TopicArn, Message, [], fun void_response/3).

publish_detail(TopicArn, Message) ->
    publish_request(TopicArn, Message, [], fun publish_response_detail/3).

publish(TopicArn, Message, Subject) ->
    Params = [{"Subject", Subject}],
    publish_request(TopicArn, Message, Params,  fun void_response/3).

publish_detail(TopicArn, Message, Subject) ->
    Params = [{"Subject", Subject}],
    publish_request(TopicArn, Message, Params,  fun publish_response_detail/3).

publish_request(TopicArn, Message, Params, Response) ->
    RequestParams = Params ++
        [{"TopicArn", TopicArn},
         {"Message", Message},
         {"Action", "Publish"}],
    request(post, AWS_SNS_URL, RequestParams, Response).

publish_response_detail(_Headers, RequestId, Doc) ->
    MessageId = aws_xpath:value("//MessageId", Doc),
    {ok, [RequestId, {"MessageId", MessageId}]}.

%% Get Topic Attributes

get_topic_attributes(TopicArn) ->
    get_topic_attributes_request(TopicArn, [], fun get_topic_attributes_response/3).

get_topic_attributes_detail(TopicArn) ->
    get_topic_attributes_request(TopicArn, [], fun get_topic_attributes_response_detail/3).

get_topic_attributes_request(TopicArn, Params, Response) ->
    RequestParams = Params ++
        [{"TopicArn", TopicArn},
         {"Action", "GetTopicAttributes"}],
    request(post, AWS_SNS_URL, RequestParams, Response).

get_topic_attributes_response(_Headers, _RequestId, Doc) ->
    Attributes = [{aws_xpath:value("./key", Entry),
                   aws_xpath:value("./value", Entry)} ||
                     Entry <- aws_xpath:nodes("//entry", Doc)],
    {ok, Attributes}.

get_topic_attributes_response_detail(_Headers, RequestId, Doc) ->
    Attributes = [{aws_xpath:value("./key", Entry),
                   aws_xpath:value("./value", Entry)} ||
                     Entry <- aws_xpath:nodes("//entry", Doc)],
    {ok, [RequestId, {"Attributes", Attributes}]}.

%% Set Topic Attributes

set_topic_attributes(TopicArn, Name, Value) ->
    set_topic_attributes_request(TopicArn, Name, Value, [], fun void_response/3).

set_topic_attributes_detail(TopicArn, Name, Value) ->
    set_topic_attributes_request(TopicArn, Name, Value, [], fun void_response_detail/3).

set_topic_attributes_request(TopicArn, Name, Value, Params, Response) ->
    RequestParams = Params ++
        [{"TopicArn", TopicArn},
         {"AttributeName", Name},
         {"AttributeValue", Value},
         {"Action", "SetTopicAttributes"}],
    request(post, AWS_SNS_URL, RequestParams, Response).

%% Add Permission

add_permission(TopicArn, Label, AWSAccountId, ActionName) ->
    Params = [{"AWSAccountId", AWSAccountId},
              {"ActionName", ActionName}],
    add_permission_request(TopicArn, Label, Params, fun void_response/3).

add_permission_detail(TopicArn, Label, AWSAccountId, ActionName) ->
    Params = [{"AWSAccountId", AWSAccountId},
              {"ActionName", ActionName}],
    add_permission_request(TopicArn, Label, Params, fun void_response_detail/3).

add_permission_request(TopicArn, Label, Params, Response) ->
    RequestParams = Params ++
        [{"TopicArn", TopicArn},
         {"Label", Label},
         {"Action", "AddPermission"}],
    request(post, AWS_SNS_URL, RequestParams, Response).

%% Remove Permission

remove_permission(TopicArn, Label) ->
    remove_permission_request(TopicArn, Label, [], fun void_response/3).

remove_permission_detail(TopicArn, Label) ->
    remove_permission_request(TopicArn, Label, [], fun void_response_detail/3).

remove_permission_request(TopicArn, Label, Params, Response) ->
    RequestParams = Params ++
        [{"TopicArn", TopicArn},
         {"Label", Label},
         {"Action", "RemovePermission"}],
    request(post, AWS_SNS_URL, RequestParams, Response).

%% Misc

void_response(_Headers, _RequestId, _Doc) ->
    ok.

void_response_detail(_Headers, RequestId, _Doc) ->
    {ok, [RequestId]}.

request(Method, Url, Params, ResponseFun) ->
    RequestParams = Params ++ 
        [{"Version", "2010-03-31"}],
    NResponseFun = 
        fun({{_, Code, _}, Headers, Body}) when Code >= 200, Code < 300 ->
            Doc = aws_xpath:scan(Body),
            RequestId = aws_xpath:value("//RequestId", Doc),
            ResponseFun(Headers, {"RequestId", RequestId}, Doc);
           ({Status, Headers, Body}) ->
            Doc = aws_xpath:scan(Body),
            RequestId = aws_xpath:value("//RequestId", Doc),
            Code = aws_xpath:value("//Code", Doc),
            Message = aws_xpath:value("//Message", Doc),
            {error, Headers ++
                 [{"RequestId", RequestId}, 
                  {"Status", Status},
                  {"Code", Code}, 
                  {"Message", Message}]}
        end,
    ?AWS_QUERY:request(Method, Url, RequestParams, NResponseFun).
