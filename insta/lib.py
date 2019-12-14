import asyncio
import json
import logging
import os
from copy import deepcopy
from random import shuffle, randint

import datetime
import shutil
import time
from itertools import chain

from insta.helpers import divide_chunks, wrap, dget
from instabot import API
from instabot.api import config
from instabot.api import devices as i_devices
from instabot.api.api import Response429, Response4xx
from instabot.api.api_login import load_uuid_and_cookie_dict
from instabot.api.devices import DEVICES

logger = logging.getLogger()

proxy = None

# proxy = 'http://5.137.218.84:8080'
# proxy = 'https://212.17.19.19:8080'
devices = list(DEVICES.keys())

# patch config
config.IG_SIG_KEY = 'a86109795736d73c9a94172cd9b736917d7d94ca61c9101164894b3f0d43bef4'
# config.REQUEST_HEADERS['X-IG-Capabilities'] = '3brTvw=='
i_devices.INSTAGRAM_VERSION = '117.0.0.28.123'

for device in DEVICES.values():
    device['instagram_version'] = i_devices.INSTAGRAM_VERSION

MAX_STORIES = 200
MAX_USER_REELS_PACK = 30

MAX_USERS_STORIES_PER_INTERVAL = 500
USERS_STORIES_INTERVAL = 60


class NeedChallenge(Exception):
    pass


class InstaLib:
    _login_url = 'https://www.instagram.com/accounts/login/?source=auth_switcher'

    targets = []
    timers = {
        'followers': [],
        'reels': [],
        'graph_followers': [],
        'views': [],
    }

    lock = False

    limits = {
        'followers': {'interval': 60, 'max': 15},
        'reels': {'interval': 60, 'max': 60},
        'views': {'interval': 60, 'max': 60},
        'graph_followers': {'interval': 60, 'max': 17},
    }

    users = set()
    proxy_session_id = None

    def __init__(self, username, password, proxy=None, base_path=''):
        self.username = username
        self.password = password
        self.base_path = base_path or './bot_sessions'

        self.remove_base_path()
        self.proxy = proxy

        if not os.path.exists(self.base_path):
            os.makedirs(self.base_path)

        print(f'init {username} proxy:{proxy}')
        self.api = API(base_path=self.base_path, save_logfile=False, device=devices[0])
        self.api.proxy = self.proxy

    def remove_base_path(self):
        try:
            shutil.rmtree(self.base_path)
        except Exception:
            pass

    proxy_index = -1

    def get_proxy(self):
        # print('No Proxy')

        return None

    async def vote_story(self, story, variant=None):
        story_id = story['id']

        pool_id = dget(story, 'story_polls.0.poll_sticker.poll_id')
        if not pool_id and dget(story, 'story_polls.0.poll_sticker.viewer_can_vote'):
            return

        variant = randint(0, 1) if variant is None else variant

        url = f'https://i.instagram.com/api/v1/media/{story_id}/{pool_id}/story_poll_vote/'

        data = self.api.json_data(
            {
                "_csrftoken": self.api.token,
                "_uuid": self.api.uuid,
                "_uid": self.api.user_id,
                'vote': str(variant),
                'radio_type': 'wifi-none'
            }
        )
        data = self.api.generate_signature(data)

        return await wrap(lambda: self.api.session.post(url, data))()

    async def vote_slider(self, story, variant=None):
        slider_id = dget(story, 'story_sliders.0.slider_sticker.slider_id')
        if not slider_id and dget(story, 'story_sliders.0.slider_sticker.viewer_can_vote'):
            return

        story_id = story['id']
        url = f'https://i.instagram.com/api/v1/media/{story_id}/{slider_id}/story_slider_vote/'

        variant = 0.8 + randint(0, 200) / 1000 if variant is None else variant

        data = self.api.json_data(
            {
                "_csrftoken": self.api.token,
                "_uuid": self.api.uuid,
                "_uid": self.api.user_id,
                'vote': str(variant),
                'radio_type': 'wifi-none'
            }
        )
        data = self.api.generate_signature(data)

        return await wrap(lambda: self.api.session.post(url, data))()

    async def quiz_story(self, story, variant=None):

        quiz_id = dget(story, 'story_quizs.0.quiz_sticker.quiz_id')
        if not quiz_id:
            return

        tallies = dget(story, 'story_quizs.0.quiz_sticker.tallies')
        if not isinstance(tallies, list):
            return

        correct = dget(story, 'story_quizs.0.quiz_sticker.correct_answer')

        variant = correct if variant is None else correct

        story_id = story['pk']

        url = f'https://i.instagram.com/api/v1/media/{story_id}/{quiz_id}/story_quiz_answer/'

        data = {
            "_csrftoken": self.api.token,
            "_uuid": self.api.uuid,
            'answer': variant,
        }
        # data = self.api.generate_signature(data)

        return await wrap(lambda: self.api.session.post(url, data))()

    async def apply_countdown(self, story):
        countdown_id = dget(story, 'story_countdowns.0.countdown_sticker.countdown_id')
        enabled = dget(story, 'story_countdowns.0.countdown_sticker.following_enabled')

        if not enabled:
            return False

        url = f'https://i.instagram.com/api/v1/media/{countdown_id}/follow_story_countdown/'

        data = self.api.json_data({

        })
        data = self.api.generate_signature(data)
        return await wrap(lambda: self.api.session.post(url, data))()

    async def answer_question(self, story, answer):
        story_id = story['id']

        question_id = dget(story, 'story_questions.0.question_sticker.question_id')
        if not question_id:
            return

        q_type = dget(story, 'story_questions.0.question_sticker.question_type')
        if q_type != 'text':
            return

        url = f'https://i.instagram.com/api/v1/media/{story_id}/{question_id}/story_question_response/'

        data = self.api.json_data(
            {
                "_csrftoken": self.api.token,
                "client_context": self.api.generate_UUID(True),
                "mutation_token": self.api.generate_UUID(True),
                "_uuid": self.api.uuid,
                "type": "text",
                "_uid": self.api.user_id,
                'response': answer,
            }
        )
        data = self.api.generate_signature(data)

        return await wrap(lambda: self.api.session.post(url, data))()

    # return $this->ig->request("media/{$storyId}/{$pollId}/story_poll_vote/")
    # ->addPost('_uuid', $this->ig->uuid)
    # ->addPost('_uid', $this->ig->account_id)
    # ->addPost('_csrftoken', $this->ig->client->getToken())
    # ->addPost('radio_type', 'wifi-none')
    # ->addPost('vote', $votingOption)
    # ->getResponse(new
    # Response\ReelMediaViewerResponse());

    async def login(self, ask_code=False):
        # get proxy
        # proxy = await self.get_proxy()
        login_kwargs = {
            'username': self.username,
            'password': self.password,
            'ask_for_code': False,
            # 'is_threaded': True,
            'proxy': self.proxy
        }

        await wrap(lambda: self.api.login(**login_kwargs))()

        if self.api.is_logged_in:
            return True

        if self.api.last_json.get("error_type", "") == "checkpoint_challenge_required":
            raise NeedChallenge()
        return False

    async def get_profile(self):
        profile = await wrap(lambda: self.api.get_profile_data())()
        if self.api.last_json:
            return self.api.last_json

    async def set_profile(self, url,
                          phone,
                          biography,
                          email,
                          gender):
        await wrap(lambda: self.api.edit_profile(url,
                                                 phone,
                                                 biography,
                                                 email,
                                                 gender))()
        if self.api.last_json:
            return self.api.last_json

    async def login_by_session(self, session_data):
        self.api.set_user(self.username, self.password)
        self.api.renew_session()
        load_uuid_and_cookie_dict(self.api, session_data)

    async def ask_variants(self):
        return await wrap(lambda: self.api.get_choices())()

    async def send_choice(self, url, code):
        return await wrap(lambda: self.api.make_choice(url, code=code))()

    async def send_code(self, url, code):
        return await wrap(lambda: self.api.send_code(url, code=code))()

    async def add_target(self, target):
        self.targets.append(target)

    async def get_user_id(self, target):
        f_cache_path = f'{self.base_path}/user_id_{target}'
        try:
            if os.path.exists(f_cache_path):
                f = open(f_cache_path, "r")
                user_id = int(f.read())
                f.close()

                return user_id
        except Exception:
            pass

        while True:
            print(f'get id {target}')
            try:
                self.api.search_username(target)
                break
            except Response429:
                # self.api.set_proxy(self.get_proxy())
                print('Wait 1m')
                await asyncio.sleep(60)
                continue

        if not self.api.last_response or self.api.last_response.status_code // 100 >= 4:
            raise Response4xx()

        if 'user' not in self.api.last_json:
            return None

        f = open(f_cache_path, "w")
        f.write(str(self.api.last_json["user"]["pk"]))
        f.close()

        return self.api.last_json["user"]["pk"]

    async def extract_users(self):
        all_f = set()
        for target in self.targets:

            user_id = await self.get_user_id(target)
            if not user_id:
                continue

            f_cache_path = f'{self.base_path}/tmp_f_{user_id}'
            if False and os.path.exists(f_cache_path):
                f = open(f_cache_path, "r")
                self.followers = json.loads(f.read())
                f.close()
                all_f.update(self.followers)
                continue

            print('Fetching Followers')
            # TODO: Fetch all
            followers = []
            next_max_id = None
            minimal_interval = 0.1
            counter = 0
            while True:
                st = time.time()
                counter += 1
                try:
                    await wrap(lambda: self.api.get_user_followers(user_id, max_id=next_max_id))()
                except Response429:
                    print(f'wait 1m {counter}')
                    await asyncio.sleep(60)
                    # self.api.set_proxy(self.get_proxy())
                    counter = 0
                    continue
                int = time.time() - st
                if int < minimal_interval:
                    print('wait')
                    await asyncio.sleep(minimal_interval - int)
                followers += [x['pk'] for x in self.api.last_json['users']]
                next_max_id = self.api.last_json.get('next_max_id')
                if not next_max_id or len(followers) >= 100000:
                    break
                await asyncio.sleep(0.2)
                print('fetched %s followers' % (len(followers)))

            f = open(f_cache_path, "w")
            f.write(json.dumps(followers))
            f.close()
            self.followers = followers
            all_f.update(followers)
            print(f'Fetched {target}')
        self.all_f = all_f

    async def check_interval(self, t):
        timers = self.timers[t]
        max_requests_per_interval = self.limits[t]['max']
        check_interval = self.limits[t]['interval']

        if len(timers) >= max_requests_per_interval:
            first = timers.pop(0)
            delta = time.time() - first
            wait = max(check_interval - delta, 0)
            if wait > 0:
                print(f'Wait {wait} {t}')
                await asyncio.sleep(wait)

        timers.append(time.time())

    async def get_reels(self, c, api=None):
        api = api or self.api
        await self.check_interval('reels')

        await wrap(lambda: api.get_users_reel(c))()

        if not self.api.last_response or self.api.last_response.status_code // 100 >= 4:
            raise Response4xx()

    async def view_reels(self, c, api=None):
        api = api or self.api
        await self.check_interval('views')

        await wrap(lambda: api.see_reels(c))()

        if not self.api.last_response or self.api.last_response.status_code // 100 >= 4:
            raise Response4xx()

    async def get_followers(self, user_id, max_id=None, api=None):
        api = api or self.api
        await self.check_interval('followers')

        await wrap(lambda: api.get_user_followers(user_id, max_id=max_id))()

        if not self.api.last_response or self.api.last_response.status_code // 100 >= 4:
            raise Response4xx()

    async def async_get_graph_followers(self, user_id, next_max_id=None):
        await self.check_interval('graph_followers')
        return await wrap(lambda: self.get_graph_followers(user_id, next_max_id=next_max_id))()

    def get_graph_followers(self, user_id, next_max_id=None):
        url = "https://www.instagram.com/graphql/query/?query_hash=56066f031e6239f35a904ac20c9f37d9&variables="
        variables = {"id": str(user_id), "first": 50, "include_reel": True, "fetch_mutual": True}

        if next_max_id:
            variables['after'] = next_max_id

        url = url + json.dumps(variables)

        logger.info(f'Fetching Followers {user_id}')
        response = self.api.session.get(url)
        return response.json()

    async def extract_users_reels(self):
        _M = {'users_reels': [], 'Watched': 0, 'reels': [], 'watching_reels': True, 'lock': False, 'watched_users': 0}

        async def get_reels():
            api = API(base_path=f'./wdir', save_logfile=False)
            api.login(username=self.username, password=self.password, proxy=self.api.proxy)
            while _M['watching_reels'] or len(_M['users_reels']):
                if _M['lock'] or (len(_M['reels']) > 200):
                    await asyncio.sleep(.01)
                    continue
                if len(_M['users_reels']) >= MAX_USER_REELS_PACK or not _M['watching_reels']:
                    c = deepcopy(_M['users_reels'][:MAX_USER_REELS_PACK])
                    _M['users_reels'] = _M['users_reels'][MAX_USER_REELS_PACK:]
                    print('Getting users reels')
                    while True:
                        try:
                            await self.get_reels(c, api=api)
                            break
                        except Response429:
                            # api.set_proxy(self.get_proxy())
                            print('wait 60s mark')
                            await asyncio.sleep(60)
                            continue

                    vals = api.last_json['reels'].values()
                    not_seen = []
                    for user in vals:
                        if user['seen'] or user['user']['is_private']:
                            continue
                        not_seen.append(user)
                    new_list = list(chain(*[x['items'][-5:] for x in not_seen]))

                    _M['reels'].extend(new_list)
                    print('Getted reels %s in query: %s' % (len(new_list), len(_M['reels'])))
                await asyncio.sleep(0)

        async def watcher():
            api = API(base_path=f'./wdir', save_logfile=False)
            api.login(username=self.username, password=self.password, proxy=self.api.proxy)
            w_counter = 0
            while _M['watching_reels'] or len(_M['reels']):
                if _M['lock']:
                    await asyncio.sleep(.01)
                    continue
                if len(_M['reels']) >= MAX_STORIES or not _M['watching_reels']:
                    c = deepcopy(_M['reels'][:MAX_STORIES])
                    user_ids = {x['user']['pk'] for x in c}
                    _M['watched_users'] += len(user_ids)
                    w_counter += len(user_ids)
                    _M['reels'] = _M['reels'][MAX_STORIES:]
                    print('Watching users reels')
                    while True:
                        try:
                            await self.view_reels(c, api=api)
                            break
                        except Response429:
                            # api.set_proxy(self.get_proxy())
                            print('wait 60s mark')
                            await asyncio.sleep(60)
                            continue
                    _M['Watched'] += len(c)
                    print('Watched: %s of %s' % (_M['Watched'], _M['watched_users']))

                    if w_counter >= MAX_USERS_STORIES_PER_INTERVAL:
                        w_counter = 0
                        print(f'Sleep {USERS_STORIES_INTERVAL} secs')
                        _M['lock'] = True
                        await asyncio.sleep(USERS_STORIES_INTERVAL)
                        _M['lock'] = False
                await asyncio.sleep(0)

        asyncio.ensure_future(get_reels())
        asyncio.ensure_future(watcher())

        targets = deepcopy(self.targets)
        shuffle(targets)

        for target in targets:

            user_id = await self.get_user_id(target)
            if not user_id:
                continue

            print(f'Fetching Followers {target} {user_id}')
            # TODO: Fetch all
            followers = []
            next_max_id = None
            while True:
                if _M['lock'] or len(_M['users_reels']) > 300:
                    await asyncio.sleep(.01)
                    continue

                try:
                    await self.get_followers(user_id, max_id=next_max_id)
                except Response429:
                    print(f'following limits wait 1m')
                    await asyncio.sleep(60)
                    # self.api.set_proxy(self.get_proxy())
                    continue

                followers += [x['pk'] for x in self.api.last_json['users']]
                _M['users_reels'] += [x['pk'] for x in self.api.last_json['users'] if x.get('latest_reel_media')]

                # reels.extend(x['latest_reel_media'] for x in self.api.last_json['users'])
                next_max_id = self.api.last_json.get('next_max_id')
                if not next_max_id:
                    break
                print('fetched %s followers with_reels %s' % (len(followers), len(_M['users_reels'])))

            print(f'Fetched {target}')

        _M['watching_reels'] = False
        while _M['users_reels'] or _M['reels']:
            await asyncio.sleep(.1)

    async def watch_stories(self):
        f_count = len(self.all_f)
        ff = list(self.all_f)
        # shuffle(ff)
        chunks = list(divide_chunks(ff, 30))
        del ff
        counters = {
            'watch': 0,
            'users': 0,
            'chunks_number': len(chunks)
        }
        st = time.time()

        check_interval = 60.0
        max_requests_per_interval = 100

        # minimal_request_interval = 0.5

        timers = []
        L = {'L': False, 'Watched': 0, 'Users': 0, 'StartTime': time.time()}

        async def logic(worker_num):
            api = API(base_path=f'./wdir', save_logfile=False, device=devices[worker_num + 1])
            _M = {'cr': []}
            _M['cr'] = _M['cr']
            api.login(username=self.username, password=self.password, ask_for_code=True, proxy=self.api.proxy)

            async def mark_reels():
                if _M['cr']:
                    user_ids = {x['user']['pk'] for x in _M['cr']}
                    counters['users'] += len(user_ids)
                    counters['watch'] += len(_M['cr'])
                    c = deepcopy(_M['cr'][:MAX_STORIES])
                    L['Watched'] += len(c)
                    _M['cr'] = _M['cr'][MAX_STORIES:]
                    while True:
                        try:
                            await wrap(lambda: api.see_reels(c))()
                            break
                        except Response429:
                            # api.set_proxy(self.get_proxy())
                            print('wait 60s mark')
                            await asyncio.sleep(60)
                            continue
                    print('Marked')

            while chunks:
                num = (counters['chunks_number'] - len(chunks)) + 1
                st = time.time()
                chunk = chunks.pop()

                while True:
                    if L['L']:
                        await asyncio.sleep(0.1)
                        continue
                    try:
                        timers.append(time.time())
                        await wrap(lambda: api.get_users_reel(chunk))()
                        L['Users'] += len(chunk)
                    except Response429:
                        # api.set_proxy(self.get_proxy())
                        print('wait 5m')
                        await asyncio.sleep(60 * 5)
                        continue
                    if api.last_response.status_code // 100 >= 4:
                        raise Response4xx
                        continue
                    vals = api.last_json['reels'].values()
                    break

                not_seen = []
                for user in vals:
                    if user['seen'] or user['user']['is_private']:
                        continue
                    not_seen.append(user)

                new_list = list(chain(*[x['items'] for x in not_seen]))

                _M['cr'].extend(new_list)
                chunk_time = round(time.time() - st, 3)

                wait = 0
                if len(timers) >= max_requests_per_interval:
                    first = timers.pop(0)
                    delta = time.time() - first
                    wait = max(check_interval - delta, 0)

                reels = len(_M['cr'])

                # approximates
                percent = num / counters["chunks_number"]
                a_u = (counters['users'] * (1 / percent))
                a_w = (L["Watched"] * (1 / percent))

                total_time = time.time() - L["StartTime"]
                et = round(total_time / num * (counters["chunks_number"] - num))
                et = str(datetime.timedelta(seconds=et))
                ft = round(total_time)
                ft = str(datetime.timedelta(seconds=ft))

                d_u = int((counters['users'] / total_time) * 86400)
                d_w = int((L["Watched"] / total_time) * 86400)

                a_u = int(a_u)
                a_w = int(a_w)

                print(
                    f'[{worker_num}] {num}/{counters["chunks_number"]} %{percent:.2f} T: {chunk_time} q: {reels} W: {L["Watched"]} A: {a_u}/{a_w} D: {d_u}/{d_w} delay: {wait} time: {ft}/{et}')

                if len(_M['cr']) >= MAX_STORIES:
                    asyncio.ensure_future(mark_reels())

                if wait:
                    L['L'] = True
                    await asyncio.sleep(wait)
                    L['L'] = False

                # if num % per_chunks == 0:
                #     self.api.set_proxy(self.get_proxy())

                # if chunk_time < minimal_request_interval:
                #     print('wait')
                #     await asyncio.sleep(minimal_request_interval - chunk_time)
                #

            await mark_reels()

        workers_count = 1
        [x for x in await asyncio.gather(*[logic(x) for x in range(workers_count)])]

        total_time = round(time.time() - st, 3)
        print(
            f'stories: {counters["watch"]} of {counters["users"]} users total time: {total_time} followers: {f_count}')
