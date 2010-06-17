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

-module(aws_sqs, [AWS_SQS_URL, AWS_ACCESS_KEY, AWS_SECRET_KEY]).

-export([create_queue/1, create_queue_detail/1, create_queue/2, create_queue_detail/2, create_queue_request/3,
         delete_queue/1, delete_queue_detail/1, delete_queue_request/3,
         list_queues/0, list_queues_detail/0, list_queues/1, list_queues_detail/1, list_queues_request/2,
         set_queue_attributes/3, set_queue_attributes_detail/3, set_queue_attributes_request/3,
         get_queue_attributes/2, get_queue_attributes_detail/2, get_queue_attributes_request/3,
         send_message/2, send_message_detail/2, send_message_request/4,
         receive_message/1, receive_message_detail/1, receive_message/2, receive_message_detail/2, receive_message_request/3,
         delete_message/2, delete_message_detail/2, delete_message_request/4,
         change_message_visibility/3, change_message_visibility_detail/3, change_message_visibility_request/5,
         add_permission/4, add_permission_detail/4, add_permission_request/4,
         remove_permission/2, remove_permission_detail/2, remove_permission_request/4]).

-define(AWS_QUERY, (aws_query:new(AWS_ACCESS_KEY, AWS_SECRET_KEY))).

%% Create Queue

create_queue(QueueName) ->
    create_queue_request(QueueName, [], fun create_queue_response/3).

create_queue_detail(QueueName) ->
    create_queue_request(QueueName, [], fun create_queue_response_detail/3).

create_queue(QueueName, VisibilityTimeout) ->
    Params = [{"DefaultVisibilityTimeout", VisibilityTimeout}],
    create_queue_request(QueueName, Params, fun create_queue_response/3).

create_queue_detail(QueueName, VisibilityTimeout) ->
    Params = [{"DefaultVisibilityTimeout", VisibilityTimeout}],
    create_queue_request(QueueName, Params, fun create_queue_response_detail/3).

create_queue_request(QueueName, Params, Response) ->
    RequestParams = Params ++
        [{"QueueName", QueueName},
         {"Action", "CreateQueue"}],
    request(post, AWS_SQS_URL, RequestParams, Response).

create_queue_response(_Headers, _RequestId, Doc) ->
    QueueUrl = aws_xpath:value("//QueueUrl", Doc),
    {ok, QueueUrl}.

create_queue_response_detail(_Headers, RequestId, Doc) ->
    QueueUrl = aws_xpath:value("//QueueUrl", Doc),
    {ok, [RequestId, {"QueueUrl", QueueUrl}]}.

%% Delete Queue

delete_queue(QueueUrl) ->
    delete_queue_request(QueueUrl, [], fun void_response/3).

delete_queue_detail(QueueUrl) ->
    delete_queue_request(QueueUrl, [], fun void_response_detail/3).

delete_queue_request(QueueUrl, Params, Response) ->
    RequestParams = Params ++
        [{"Action", "DeleteQueue"}],
    request(post, QueueUrl, RequestParams, Response).

%% List Queues

list_queues() ->
    list_queues_request([], fun list_queues_response/3).

list_queues_detail() ->
    list_queues_request([], fun list_queues_response_detail/3).

list_queues(QueueNamePrefix) ->
    Params = [{"QueueNamePrefix", QueueNamePrefix}],
    list_queues_request(Params, fun list_queues_response/3).

list_queues_detail(QueueNamePrefix) ->
    Params = [{"QueueNamePrefix", QueueNamePrefix}],
    list_queues_request(Params, fun list_queues_response_detail/3).

list_queues_request(Params, Response) ->
    RequestParams = Params ++
        [{"Action", "ListQueues"}],
    request(post, AWS_SQS_URL, RequestParams, Response).

list_queues_response(_Headers, _RequestId, Doc) ->
    QueueUrls = aws_xpath:values("//QueueUrl", Doc),
    {ok, QueueUrls}.

list_queues_response_detail(_Headers, RequestId, Doc) ->
    QueueUrls = aws_xpath:values("//QueueUrl", Doc),
    {ok, [RequestId, {"QueueUrls", QueueUrls}]}.

%% Set Queue Attributes

set_queue_attributes(QueueUrl, Name, Value) ->
    Params = [{"Attribute.Name", Name},
              {"Attribute.Value", Value}],
    set_queue_attributes_request(QueueUrl, Params, fun void_response/3).

set_queue_attributes_detail(QueueUrl, Name, Value) ->
    Params = [{"Attribute.Name", Name},
              {"Attribute.Value", Value}],
    set_queue_attributes_request(QueueUrl, Params, fun void_response_detail/3).

set_queue_attributes_request(QueueUrl, Params, Response) ->
    RequestParams = Params ++
        [{"Action", "SetQueueAttributes"}],
    request(post, QueueUrl, RequestParams, Response).

%% Get Queue Attributes

get_queue_attributes(QueueUrl, Name) ->
    Params = [{"AttributeName", Name}],
    get_queue_attributes_request(QueueUrl, Params, fun get_queue_attributes_response/3).

get_queue_attributes_detail(QueueUrl, Name) ->
    Params = [{"AttributeName", Name}],
    get_queue_attributes_request(QueueUrl, Params, fun get_queue_attributes_response_detail/3).

get_queue_attributes_request(QueueUrl, Params, Response) ->
    RequestParams = Params ++
        [{"Action", "GetQueueAttributes"}],
    request(post, QueueUrl, RequestParams, Response).

get_queue_attributes_response(_Headers, _RequestId, Doc) ->
    Attributes = [{aws_xpath:value("./Name", Attribute), 
                   aws_xpath:value("./Value", Attribute)} ||
                     Attribute <- aws_xpath:nodes("//Attribute", Doc)],
    {ok, Attributes}.

get_queue_attributes_response_detail(_Headers, RequestId, Doc) ->
    Attributes = [{aws_xpath:value("./Name", Attribute), 
                   aws_xpath:value("./Value", Attribute)} ||
                     Attribute <- aws_xpath:nodes("//Attribute", Doc)],
    {ok, [RequestId, {"Attributes", Attributes}]}.

%% Send Message

send_message(QueueUrl, MessageBody) ->
    send_message_request(QueueUrl, MessageBody, [], fun void_response/3).

send_message_detail(QueueUrl, MessageBody) ->
    send_message_request(QueueUrl, MessageBody, [], fun send_message_response_detail/3).

send_message_request(QueueUrl, MessageBody, Params, Response) ->
    RequestParams = Params ++
        [{"MessageBody", MessageBody},
         {"Action", "SendMessage"}],
    request(post, QueueUrl, RequestParams, Response).

send_message_response_detail(_Headers, RequestId, Doc) ->
    MessageId = aws_xpath:value("//MessageId", Doc),
    MessageMD5 = aws_xpath:value("//MD5OfMessageBody", Doc),
    {ok, [RequestId, {"MessageId", MessageId}, {"MessageMD5", MessageMD5}]}.

%% Receive Message

receive_message(QueueUrl) ->
    receive_message_request(QueueUrl, [], fun receive_message_response/3).

receive_message_detail(QueueUrl) ->
    receive_message_request(QueueUrl, [], fun receive_message_response_detail/3).

receive_message(QueueUrl, NumberOfMessages) ->
    Params = [{"MaxNumberOfMessages", NumberOfMessages}],
    receive_message_request(QueueUrl, Params, fun receive_message_response/3).

receive_message_detail(QueueUrl, NumberOfMessages) ->
    Params = [{"MaxNumberOfMessages", NumberOfMessages}],
    receive_message_request(QueueUrl, Params, fun receive_message_response_detail/3).

receive_message_request(QueueUrl, Params, Response) ->
    RequestParams = Params ++
        [{"Action", "ReceiveMessage"}],
    request(post, QueueUrl, RequestParams, Response).

receive_message_response(_Headers, _RequestId, Doc) ->
    Messages = [{aws_xpath:value("./ReceiptHandle", Message), 
                 aws_xpath:value("./Body", Message)} ||
                   Message <- aws_xpath:nodes("//Message", Doc)],
    {ok, Messages}.

receive_message_response_detail(_Headers, RequestId, Doc) ->
    Messages = [[{"MessageId", aws_xpath:value("./MessageId", Message)},
                 {"MessageMD5", aws_xpath:value("./MD5OfBody", Message)},
                 {"ReceiptHandle", aws_xpath:value("./ReceiptHandle", Message)},
                 {"Attributes", [{aws_xpath:value("./Name", Attribute), 
                                  aws_xpath:value("./Value", Attribute)} ||
                                    Attribute <- aws_xpath:nodes("./Attribute", Message)]},
                 {"Body", aws_xpath:value("./Body", Message)}] ||
                   Message <- aws_xpath:nodes("//Message", Doc)],
    {ok, [RequestId, {"Messages", Messages}]}.

%% Delete Message

delete_message(QueueUrl, ReceiptHandle) ->
    delete_message_request(QueueUrl, ReceiptHandle, [], fun void_response/3).

delete_message_detail(QueueUrl, ReceiptHandle) ->
    delete_message_request(QueueUrl, ReceiptHandle, [], fun void_response_detail/3).

delete_message_request(QueueUrl, ReceiptHandle, Params, Response) ->
    RequestParams = Params ++
        [{"ReceiptHandle", ReceiptHandle},
         {"Action", "DeleteMessage"}],
    request(post, QueueUrl, RequestParams, Response).

%% Change Message Visibility

change_message_visibility(QueueUrl, ReceiptHandle, VisibilityTimeout) ->
    change_message_visibility_request(QueueUrl, ReceiptHandle, VisibilityTimeout, [], fun void_response/3).

change_message_visibility_detail(QueueUrl, ReceiptHandle, VisibilityTimeout) ->
    change_message_visibility_request(QueueUrl, ReceiptHandle, VisibilityTimeout, [], fun void_response_detail/3).

change_message_visibility_request(QueueUrl, ReceiptHandle, VisibilityTimeout, Params, Response) ->
    RequestParams = Params ++
        [{"ReceiptHandle", ReceiptHandle},
         {"VisibilityTimeout", VisibilityTimeout},
         {"Action", "ChangeMessageVisibility"}],
    request(post, QueueUrl, RequestParams, Response).

%% Add Permission

add_permission(QueueUrl, Label, AWSAccountId, ActionName) ->
    Params = [{"AWSAccountId", AWSAccountId},
              {"ActionName", ActionName}],
    add_permission_request(QueueUrl, Label, Params, fun void_response/3).

add_permission_detail(QueueUrl, Label, AWSAccountId, ActionName) ->
    Params = [{"AWSAccountId", AWSAccountId},
              {"ActionName", ActionName}],
    add_permission_request(QueueUrl, Label, Params, fun void_response_detail/3).

add_permission_request(QueueUrl, Label, Params, Response) ->
    RequestParams = Params ++
        [{"Label", Label},
         {"Action", "AddPermission"}],
    request(post, QueueUrl, RequestParams, Response).

%% Remove Permission

remove_permission(QueueUrl, Label) ->
    remove_permission_request(QueueUrl, Label, [], fun void_response/3).

remove_permission_detail(QueueUrl, Label) ->
    remove_permission_request(QueueUrl, Label, [], fun void_response_detail/3).

remove_permission_request(QueueUrl, Label, Params, Response) ->
    RequestParams = Params ++
        [{"Label", Label},
         {"Action", "RemovePermission"}],
    request(post, QueueUrl, RequestParams, Response).

%% Misc

void_response(_Headers, _RequestId, _Doc) ->
    ok.

void_response_detail(_Headers, RequestId, _Doc) ->
    {ok, [RequestId]}.

request(Method, Url, Params, ResponseFun) ->
    RequestParams = Params ++ 
        [{"Version", "2009-02-01"}],
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
