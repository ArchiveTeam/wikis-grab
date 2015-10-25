# encoding=utf8
import datetime
from distutils.version import StrictVersion
import hashlib
import os.path
import random
from seesaw.config import realize, NumberConfigValue
from seesaw.item import ItemInterpolation, ItemValue
from seesaw.task import SimpleTask, LimitConcurrent
from seesaw.tracker import GetItemFromTracker, PrepareStatsForTracker, \
    UploadWithTracker, SendDoneToTracker
import shutil
import socket
import subprocess
import sys
import time
import string
try:
    import requests
except ImportError:
    print('Please install or update the requests module.')
    sys.exit(1)
import json
import re

import seesaw
from seesaw.externalprocess import WgetDownload
from seesaw.pipeline import Pipeline
from seesaw.project import Project
from seesaw.util import find_executable


# check the seesaw version
if StrictVersion(seesaw.__version__) < StrictVersion("0.8.5"):
    raise Exception("This pipeline needs seesaw version 0.8.5 or higher.")


###########################################################################
# Find a useful Wget+Lua executable.
#
# WGET_LUA will be set to the first path that
# 1. does not crash with --version, and
# 2. prints the required version string
WGET_LUA = find_executable(
    "Wget+Lua",
    ["GNU Wget 1.14.lua.20130523-9a5c"],
    [
        "./wget-lua",
        "./wget-lua-warrior",
        "./wget-lua-local",
        "../wget-lua",
        "../../wget-lua",
        "/home/warrior/wget-lua",
        "/usr/bin/wget-lua"
    ]
)

if not WGET_LUA:
    raise Exception("No usable Wget+Lua found.")


###########################################################################
# The version number of this pipeline definition.
#
# Update this each time you make a non-cosmetic change.
# It will be added to the WARC files and reported to the tracker.
VERSION = "20151021.01"
USER_AGENT = 'ArchiveTeam'
TRACKER_ID = 'wikis'
TRACKER_HOST = 'tracker.archiveteam.org'


###########################################################################
# This section defines project-specific tasks.
#
# Simple tasks (tasks that do not need any concurrency) are based on the
# SimpleTask class and have a process(item) method that is called for
# each item.
class CheckIP(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, "CheckIP")
        self._counter = 0

    def process(self, item):
        # NEW for 2014! Check if we are behind firewall/proxy

        if self._counter <= 0:
            item.log_output('Checking IP address.')
            ip_set = set()

            ip_set.add(socket.gethostbyname('twitter.com'))
            ip_set.add(socket.gethostbyname('facebook.com'))
            ip_set.add(socket.gethostbyname('youtube.com'))
            ip_set.add(socket.gethostbyname('microsoft.com'))
            ip_set.add(socket.gethostbyname('icanhas.cheezburger.com'))
            ip_set.add(socket.gethostbyname('archiveteam.org'))

            if len(ip_set) != 6:
                item.log_output('Got IP addresses: {0}'.format(ip_set))
                item.log_output(
                    'Are you behind a firewall/proxy? That is a big no-no!')
                raise Exception(
                    'Are you behind a firewall/proxy? That is a big no-no!')

        # Check only occasionally
        if self._counter <= 0:
            self._counter = 10
        else:
            self._counter -= 1


class PrepareDirectories(SimpleTask):
    def __init__(self, warc_prefix):
        SimpleTask.__init__(self, "PrepareDirectories")
        self.warc_prefix = warc_prefix

    def process(self, item):
        item_name = item["item_name"]
        escaped_item_name = item_name.replace(':', '_').replace('/', '_').replace('~', '_')
        dirname = "/".join((item["data_dir"], escaped_item_name))

        if os.path.isdir(dirname):
            shutil.rmtree(dirname)

        os.makedirs(dirname)

        item["item_dir"] = dirname
        item["warc_file_base"] = "%s-%s-%s" % (self.warc_prefix, escaped_item_name,
            time.strftime("%Y%m%d-%H%M%S"))

        open("%(item_dir)s/%(warc_file_base)s.warc.gz" % item, "w").close()


class MoveFiles(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, "MoveFiles")

    def process(self, item):
        # NEW for 2014! Check if wget was compiled with zlib support
        if os.path.exists("%(item_dir)s/%(warc_file_base)s.warc" % item):
            raise Exception('Please compile wget with zlib support!')

        os.rename("%(item_dir)s/%(warc_file_base)s.warc.gz" % item,
              "%(data_dir)s/%(warc_file_base)s.warc.gz" % item)

        shutil.rmtree("%(item_dir)s" % item)


def get_hash(filename):
    with open(filename, 'rb') as in_file:
        return hashlib.sha1(in_file.read()).hexdigest()


CWD = os.getcwd()
PIPELINE_SHA1 = get_hash(os.path.join(CWD, 'pipeline.py'))
LUA_SHA1 = get_hash(os.path.join(CWD, 'wikis.lua'))


def stats_id_function(item):
    # NEW for 2014! Some accountability hashes and stats.
    d = {
        'pipeline_hash': PIPELINE_SHA1,
        'lua_hash': LUA_SHA1,
        'python_version': sys.version,
    }

    return d


class WgetArgs(object):
    def realize(self, item):
        wget_args = [
            WGET_LUA,
            "-U", USER_AGENT,
            "-nv",
            "--lua-script", "wikis.lua",
            "-o", ItemInterpolation("%(item_dir)s/wget.log"),
            "--no-check-certificate",
            "--output-document", ItemInterpolation("%(item_dir)s/wget.tmp"),
            "--truncate-output",
            "-e", "robots=off",
            "--rotate-dns",
           # "--recursive", "--level=inf",
            "--no-parent",
            "--page-requisites",
            "--timeout", "30",
            "--tries", "inf",
           # "--domains", "google.com",
            "--span-hosts",
            "--waitretry", "30",
            "--warc-file", ItemInterpolation("%(item_dir)s/%(warc_file_base)s"),
            "--warc-header", "operator: Archive Team",
            "--warc-header", "wikis-dld-script-version: " + VERSION,
            "--warc-header", ItemInterpolation("wikis-user: %(item_name)s"),
        ]

        # example item: mediawiki:thegrishaverse.wikia.com/api.php:thegrishaverse.wikia.com/wiki/
        
        item_name = item['item_name']
        assert ':' in item_name
        item_type, item_api, item_base = item_name.split(':', 2)
        
        item['item_type'] = item_type
        item['item_api'] = item_api
        item['item_base'] = item_base

        print('API = %s'%(item_api))
        print('Pagebase = %s'%(item_base))
        
        assert item_type in ('mediawiki', 'mediawikieu')

        if item_type == 'mediawiki' or item_type == 'mediawikieu':
            # Code below is partially taken from https://github.com/WikiTeam/wikiteam/blob/master/dumpgenerator.py and may be edited
            if item_type == 'mediawiki':
                lists = ['allcategories:ac:Category:', 'allimages:ai:', 'allpages:ap:', 'allusers:au:User:']
                wget_args.append('http://%s'%(item_base))
                wget_args.append('http://%s'%(re.search(r'^([^/]+)', item_base).group(1)))
            elif item_type == 'mediawikieu':
                lists = ['exturlusage:eu:']
            for newlist in lists:
                listname, listid, pageprefix = newlist.split(':', 2)
                titles = []
                if item_type == 'mediawiki':
                    apfrom = '!'
                elif item_type == 'mediawikieu':
                    apfrom = '0'
                if listname == 'allusers' and re.search(r'[^/]*wikia\.com', item_api):
                    apfrom = ''
                while apfrom:
                    #print('%sfrom %s'%(listid, apfrom))
                    retries = 0
                    while retries < 5:
                        try:
                            if item_type == "mediawikieu":
                                html = requests.get('http://%s?action=query&list=%s&%slimit=500&format=json&%soffset=%s'%(item_api, listname, listid, listid, apfrom))
                            else:
                                html = requests.get('http://%s?action=query&list=%s&%slimit=500&format=json&%sfrom=%s'%(item_api, listname, listid, listid, apfrom))
                            break
                        except:
                            print('Connection error: %s'%(str(err)))
                            retries += 1
                            time.sleep(2)
                    if not 200 <= html.status_code < 300:
                        raise Exception('Received status code %d, aborting...'%(statuscode))
                    if html.text.startswith(u'\ufeff'):
                        html.encoding = 'utf-8-sig'
                    for newurl in re.findall(r'"(https?:\\\\/\\\\/[^"]+)"', html.text):
                        wget_args.append(newurl)
                        print(newurl)
                    jsonfile = html.json()
                    apfrom = ''
                    if 'query-continue' in jsonfile and listname in jsonfile['query-continue']:
                        if listid + 'continue' in jsonfile['query-continue'][listname]:
                            apfrom = jsonfile['query-continue'][listname][listid + 'continue']
                        elif listid + 'from' in jsonfile['query-continue'][listname]:
                            apfrom = jsonfile['query-continue'][listname][listid + 'from']
                        elif listid + 'offset' in jsonfile['query-continue'][listname]:
                            apfrom = jsonfile['query-continue'][listname][listid + 'offset']
                    allpages = jsonfile['query'][listname]
                    if isinstance(allpages, dict):
                        allpages = allpages.values()
                    for page in allpages:
                        if listname == 'allcategories':
                            titles.append(page['*'])
                            wget_args.append('http://%s%s%s'%(item_base, pageprefix, page['*']))
                        elif listname == 'allusers':
                            titles.append(page['name'])
                            wget_args.append('http://%s%s%s'%(item_base, pageprefix, page['name']))
                        elif listname == 'exturlusage':
                            titles.append(page['url'])
                            wget_args.append(page['url'])
                        else:
                            titles.append(page['title'])
                            wget_args.append('http://%s%s%s'%(item_base, pageprefix, page['title']))
                        if listname == 'allimages':
                            titles.append(page['url'])
                            wget_args.append(page['url'])
                    print('Found and queued %d URLs, continuing...'%(len(allpages)))
                    #print('%sfrom %s'%(listid, apfrom))
                    if len(titles) != len(set(titles)) and item_type != 'mediawikieu':
                        print('Probably a loop, finishing.')
                        titles = list(set(titles))
                        apfrom = ''
        else:
            raise Exception('Unknown item')
        
        if 'bind_address' in globals():
            wget_args.extend(['--bind-address', globals()['bind_address']])
            print('')
            print('*** Wget will bind address at {0} ***'.format(
                globals()['bind_address']))
            print('')
            
        return realize(wget_args, item)

###########################################################################
# Initialize the project.
#
# This will be shown in the warrior management panel. The logo should not
# be too big. The deadline is optional.
project = Project(
    title="wikis",
    project_html="""
        <img class="project-logo" alt="Project logo" src="http://archiveteam.org/images/a/a6/Wikiteam.jpg" height="50px" title=""/>
        <h2>Wikis<span class="links"><a href="http://archiveteam.org/index.php?title=WikiTeam">Website</a> &middot; <a href="http://tracker.archiveteam.org/wikis/">Leaderboard</a></span></h2>
        <p>Saving all wikis.</p>
    """
)

pipeline = Pipeline(
    CheckIP(),
    GetItemFromTracker("http://%s/%s" % (TRACKER_HOST, TRACKER_ID), downloader,
        VERSION),
    PrepareDirectories(warc_prefix="wikis"),
    WgetDownload(
        WgetArgs(),
        max_tries=2,
        accept_on_exit_code=[0, 4, 8],
        env={
            "item_dir": ItemValue("item_dir"),
            "item_type": ItemValue("item_type"),
            "item_api": ItemValue("item_api"),
            "item_base": ItemValue("item_base"),
        }
    ),
    PrepareStatsForTracker(
        defaults={"downloader": downloader, "version": VERSION},
        file_groups={
            "data": [
                ItemInterpolation("%(item_dir)s/%(warc_file_base)s.warc.gz")
            ]
        },
        id_function=stats_id_function,
    ),
    MoveFiles(),
    LimitConcurrent(NumberConfigValue(min=1, max=4, default="1",
        name="shared:rsync_threads", title="Rsync threads",
        description="The maximum number of concurrent uploads."),
        UploadWithTracker(
            "http://%s/%s" % (TRACKER_HOST, TRACKER_ID),
            downloader=downloader,
            version=VERSION,
            files=[
                ItemInterpolation("%(data_dir)s/%(warc_file_base)s.warc.gz")
            ],
            rsync_target_source_path=ItemInterpolation("%(data_dir)s/"),
            rsync_extra_args=[
                "--recursive",
                "--partial",
                "--partial-dir", ".rsync-tmp",
            ]
            ),
    ),
    SendDoneToTracker(
        tracker_url="http://%s/%s" % (TRACKER_HOST, TRACKER_ID),
        stats=ItemValue("stats")
    )
)
