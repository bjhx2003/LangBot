import asyncio
import json
import logging
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from langbot.libs.wecom_customer_service_api.api import WecomCSClient
import langbot.libs.wecom_customer_service_api.api as wecomcs_api
from langbot.pkg.platform.wecomcs.pull_worker import WecomCSPullWorker


def session_event(*, code='welcome-code', age=0):
    return {
        'msgid': 'enter-1',
        'msgtype': 'event',
        'send_time': time.time() - age,
        'event': {
            'event_type': 'enter_session',
            'open_kfid': 'kf-1',
            'external_userid': 'user-1',
            'welcome_code': code,
        },
    }


@pytest.fixture
async def welcome_api(monkeypatch):
    state = SimpleNamespace(messages=[], calls=[], responses=[{'errcode': 0, 'msgid': 'welcome-1'}], has_more=True)
    client = WecomCSClient('corp-id', 'secret', 'token', 'aes', SimpleNamespace(ap=None), unified_mode=True)
    client.access_token = 'access-token'
    client.welcome_message = '您好，请告诉我装货地和卸货地。'
    state.client = client

    async def respond(request):
        state.calls.append((request.url, json.loads(request.content)))
        if request.url.path.endswith('/sync_msg'):
            return httpx.Response(
                200,
                json={'errcode': 0, 'msg_list': state.messages, 'next_cursor': 'cursor-2', 'has_more': state.has_more},
            )
        response = state.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        if callable(response):
            return await response(request)
        return httpx.Response(200, json=response)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http_client:
        monkeypatch.setattr(client, '_get_http_client', AsyncMock(return_value=http_client))
        monkeypatch.setattr(client, 'ensure_access_token', AsyncMock(return_value='access-token'))
        yield state


@pytest.mark.asyncio
async def test_sync_page_sends_welcome_before_media_and_preserves_messages(welcome_api, monkeypatch):
    image = {'msgid': 'image-1', 'msgtype': 'image', 'image': {'media_id': 'media-1'}}
    text = {'msgid': 'text-1', 'msgtype': 'text', 'text': {'content': '询价'}}
    welcome_api.messages = [image, session_event(), text]

    async def get_picture(media_id):
        assert len(welcome_api.calls) == 2
        return 'picture-data'

    monkeypatch.setattr(welcome_api.client, 'get_pic_url', get_picture)
    page = await welcome_api.client.fetch_sync_msg_page('callback-token', 'kf-1', 'cursor-1')

    assert page == {
        'msg_list': [{**image, 'picurl': 'picture-data'}, text],
        'next_cursor': 'cursor-2',
        'has_more': True,
    }
    welcome_url, payload = welcome_api.calls[1]
    assert welcome_url.path.endswith('/kf/send_msg_on_event')
    assert welcome_url.params['access_token'] == 'access-token'
    assert payload['code'] == 'welcome-code'
    assert payload['msgtype'] == 'text'
    assert payload['text'] == {'content': welcome_api.client.welcome_message}
    assert len(payload['msgid']) == 32
    assert 'touser' not in payload
    assert 'open_kfid' not in payload


@pytest.mark.asyncio
@pytest.mark.parametrize('message', ['', ' \n ', None])
async def test_blank_welcome_keeps_callback_and_normal_messages_working(welcome_api, message):
    welcome_api.client.welcome_message = message
    text = {'msgid': 'text-1', 'msgtype': 'text', 'text': {'content': '你好'}}
    welcome_api.messages = [session_event(), text]

    page = await welcome_api.client.fetch_sync_msg_page('callback-token', 'kf-1')

    assert page['msg_list'] == [text]
    assert len(welcome_api.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('age', [20, 60, 86400])
async def test_expired_enter_event_does_not_send_welcome(welcome_api, age):
    welcome_api.messages = [session_event(age=age)]

    page = await welcome_api.client.fetch_sync_msg_page('callback-token', 'kf-1')

    assert page['msg_list'] == []
    assert len(welcome_api.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('code', ['', None])
async def test_enter_event_without_welcome_code_is_skipped(welcome_api, code):
    welcome_api.messages = [session_event(code=code)]

    page = await welcome_api.client.fetch_sync_msg_page('callback-token', 'kf-1')

    assert page['msg_list'] == []
    assert len(welcome_api.calls) == 1


@pytest.mark.asyncio
async def test_other_events_are_preserved(welcome_api):
    event = session_event()
    event['event']['event_type'] = 'session_status_change'
    welcome_api.messages = [event]

    page = await welcome_api.client.fetch_sync_msg_page('callback-token', 'kf-1')

    assert page['msg_list'] == [event]
    assert len(welcome_api.calls) == 1


@pytest.mark.asyncio
async def test_duplicate_enter_events_send_welcome_once(welcome_api):
    welcome_api.messages = [session_event()]

    await asyncio.gather(
        welcome_api.client.fetch_sync_msg_page('callback-token', 'kf-1'),
        welcome_api.client.fetch_sync_msg_page('callback-token', 'kf-1'),
    )

    assert sum(url.path.endswith('/send_msg_on_event') for url, _ in welcome_api.calls) == 1


@pytest.mark.asyncio
async def test_welcome_reservation_is_shared_through_redis(welcome_api):
    reserve = AsyncMock(side_effect=[True, False])
    welcome_api.client.token_cache.redis_mgr = SimpleNamespace(is_available=lambda: True, set_if_not_exists=reserve)
    welcome_api.messages = [session_event()]

    await welcome_api.client.fetch_sync_msg_page('callback-token', 'kf-1')
    welcome_api.client._welcome_attempts.clear()
    await welcome_api.client.fetch_sync_msg_page('callback-token', 'kf-1')

    assert reserve.await_count == 2
    assert reserve.await_args.kwargs['ex'] >= 20
    assert 'welcome-code' not in reserve.await_args.args[0]
    assert sum(url.path.endswith('/send_msg_on_event') for url, _ in welcome_api.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', [{'errcode': 40001, 'errmsg': 'bad credential'}, httpx.ReadTimeout('timeout')])
async def test_welcome_failure_does_not_block_normal_messages(welcome_api, caplog, failure):
    text = {'msgid': 'text-1', 'msgtype': 'text', 'text': {'content': '询价'}}
    welcome_api.messages = [session_event(), text]
    welcome_api.responses = [failure]

    with caplog.at_level(logging.WARNING, logger='langbot'):
        page = await welcome_api.client.fetch_sync_msg_page('callback-token', 'kf-1')

    assert page['msg_list'] == [text]
    assert 'welcome' in caplog.text.lower()
    assert 'welcome-code' not in caplog.text
    assert 'access-token' not in caplog.text


@pytest.mark.asyncio
async def test_welcome_refreshes_expired_token_once_with_same_msgid(welcome_api, monkeypatch):
    welcome_api.messages = [session_event()]
    welcome_api.responses = [{'errcode': 42001}, {'errcode': 0}]

    async def refresh_token():
        welcome_api.client.access_token = 'fresh-token'

    refresh = AsyncMock(side_effect=refresh_token)
    monkeypatch.setattr(welcome_api.client, 'refresh_access_token', refresh)
    await welcome_api.client.fetch_sync_msg_page('callback-token', 'kf-1')

    assert refresh.await_count == 1
    assert len(welcome_api.calls) == 3
    assert welcome_api.calls[2][0].params['access_token'] == 'fresh-token'
    assert welcome_api.calls[1][1] == welcome_api.calls[2][1]


@pytest.mark.asyncio
async def test_welcome_does_not_loop_on_repeated_invalid_token(welcome_api, monkeypatch):
    welcome_api.messages = [session_event()]
    welcome_api.responses = [{'errcode': 42001}, {'errcode': 42001}]
    refresh = AsyncMock()
    monkeypatch.setattr(welcome_api.client, 'refresh_access_token', refresh)

    page = await welcome_api.client.fetch_sync_msg_page('callback-token', 'kf-1')

    assert refresh.await_count == 1
    assert len(welcome_api.calls) == 3
    assert page['msg_list'] == []


@pytest.mark.asyncio
async def test_welcome_respects_remaining_code_lifetime(welcome_api):
    welcome_api.messages = [session_event(age=19.9)]

    async def slow_response(request):
        await asyncio.sleep(1)
        raise AssertionError('welcome request should have timed out')

    welcome_api.responses = [slow_response]

    page = await asyncio.wait_for(welcome_api.client.fetch_sync_msg_page('callback-token', 'kf-1'), timeout=0.5)

    assert page['msg_list'] == []
    assert len(welcome_api.calls) == 2


@pytest.mark.asyncio
async def test_welcome_content_limit_uses_utf8_bytes(welcome_api):
    welcome_api.client.welcome_message = '你' * 683
    welcome_api.messages = [session_event()]

    page = await welcome_api.client.fetch_sync_msg_page('callback-token', 'kf-1')

    assert page['msg_list'] == []
    assert len(welcome_api.calls) == 1


@pytest.mark.asyncio
async def test_official_enter_event_without_msgid_or_timestamp_sends_welcome(welcome_api):
    event = session_event()
    event.pop('send_time')
    event.pop('msgid')
    welcome_api.messages = [event]

    page = await welcome_api.client.fetch_sync_msg_page('callback-token', 'kf-1')

    assert page['msg_list'] == []
    assert len(welcome_api.calls) == 2


@pytest.mark.asyncio
async def test_millisecond_event_timestamp_is_supported(welcome_api):
    event = session_event()
    event['send_time'] *= 1000
    welcome_api.messages = [event]

    await welcome_api.client.fetch_sync_msg_page('callback-token', 'kf-1')

    assert len(welcome_api.calls) == 2


@pytest.mark.asyncio
async def test_welcome_text_at_byte_limit_preserves_whitespace(welcome_api):
    welcome_api.client.welcome_message = '\n' + '你' * 682 + '\n'
    welcome_api.messages = [session_event()]

    await welcome_api.client.fetch_sync_msg_page('callback-token', 'kf-1')

    assert welcome_api.calls[1][1]['text']['content'] == welcome_api.client.welcome_message


@pytest.mark.asyncio
async def test_redis_failure_uses_local_welcome_deduplication(welcome_api):
    welcome_api.client.token_cache.redis_mgr = SimpleNamespace(
        is_available=lambda: True, set_if_not_exists=AsyncMock(side_effect=RuntimeError('Redis unavailable'))
    )
    welcome_api.messages = [session_event()]

    await welcome_api.client.fetch_sync_msg_page('callback-token', 'kf-1')
    await welcome_api.client.fetch_sync_msg_page('callback-token', 'kf-1')

    assert sum(url.path.endswith('/send_msg_on_event') for url, _ in welcome_api.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('content', ['', '欢迎咨询'])
async def test_direct_callback_acknowledges_and_dispatches_text_with_optional_welcome(
    welcome_api, monkeypatch, content
):
    welcome_api.client.welcome_message = content
    welcome_api.messages = [
        session_event(),
        {
            'msgid': 'text-1',
            'msgtype': 'text',
            'open_kfid': 'kf-1',
            'external_userid': 'user-1',
            'text': {'content': '询价'},
        },
    ]
    handled = []

    @welcome_api.client.on_message('text')
    async def handle_text(event):
        handled.append(event.message)

    xml = '<xml><Token>callback-token</Token><OpenKfId>kf-1</OpenKfId></xml>'
    monkeypatch.setattr(wecomcs_api, 'WXBizMsgCrypt', lambda *args: SimpleNamespace(DecryptMsg=lambda *args: (0, xml)))
    request = SimpleNamespace(method='POST', args={}, data=asyncio.sleep(0, result=b'encrypted'))

    response = await welcome_api.client._handle_callback_internal(request)
    assert response == 'success'
    await asyncio.gather(*list(welcome_api.client._background_tasks))

    assert handled == ['询价']
    assert sum(url.path.endswith('/send_msg_on_event') for url, _ in welcome_api.calls) == bool(content)


@pytest.mark.asyncio
async def test_pull_worker_sends_welcome_without_putting_event_on_message_queue(welcome_api):
    text = {'msgid': 'text-1', 'msgtype': 'text', 'external_userid': 'user-1', 'text': {'content': '询价'}}
    welcome_api.messages = [session_event(), text]
    welcome_api.has_more = False
    state_store = SimpleNamespace(
        acquire_pull_lock=AsyncMock(return_value=True),
        release_pull_lock=AsyncMock(),
        get_cursor=AsyncMock(return_value='cursor-1'),
        is_bootstrapped=AsyncMock(return_value=True),
        reserve_message_for_queue=AsyncMock(return_value=True),
        mark_bootstrapped=AsyncMock(),
    )
    publish_message = AsyncMock()
    worker = WecomCSPullWorker(welcome_api.client, state_store, publish_message)

    count = await worker.handle_trigger({'bot_uuid': 'bot-1', 'open_kfid': 'kf-1', 'callback_token': 'callback-token'})

    assert count == 1
    publish_message.assert_awaited_once_with(text)
    state_store.mark_bootstrapped.assert_awaited_once_with('bot-1', 'kf-1', 'cursor-2')
    assert welcome_api.calls[1][0].path.endswith('/send_msg_on_event')
